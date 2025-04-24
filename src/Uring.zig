const Uring = @This();

const std = @import("std");
const builtin = @import("builtin");

const io = @import("main.zig");

const Allocator = std.mem.Allocator;
const Queue = @import("queue.zig").Intrusive;
const assert = std.debug.assert;
const linux = std.os.linux;
const posix = std.posix;

const common_flags: u32 =
    linux.IORING_SETUP_SUBMIT_ALL | // Keep submitting events even if one had an error
    linux.IORING_SETUP_CLAMP | // Clamp entries to system supported max
    linux.IORING_SETUP_DEFER_TASKRUN | // Defer work until we submit tasks. Requires SINGLE_ISSUER
    linux.IORING_SETUP_COOP_TASKRUN | // Don't interupt userspace when task is complete
    linux.IORING_SETUP_SINGLE_ISSUER; // Only a single thread will issue tasks

const msg_ring_received_cqe = 1 << 8;

ring: linux.IoUring,
in_flight: Queue(io.Task, .in_flight) = .{},
eventfd: ?posix.fd_t = null,

/// Initialize a Ring
pub fn init(_: Allocator, entries: u16) !Uring {
    var params = std.mem.zeroInit(linux.io_uring_params, .{
        .flags = common_flags,
        .sq_thread_idle = 1000,
    });

    return .{ .ring = try .init_params(entries, &params) };
}

pub fn deinit(self: *Uring, gpa: Allocator) void {
    while (self.in_flight.pop()) |task| gpa.destroy(task);

    if (self.ring.fd >= 0) {
        self.ring.deinit();
    }
    if (self.eventfd) |fd| {
        posix.close(fd);
        self.eventfd = null;
    }
    self.* = undefined;
}

/// Initializes a child Ring which can be woken up by self. This must be called from the thread
/// which will operate the child ring. Initializes with the same queue size as the parent
pub fn initChild(self: Uring, entries: u16) !Uring {
    const flags: u32 = common_flags | linux.IORING_SETUP_ATTACH_WQ;

    var params = std.mem.zeroInit(linux.io_uring_params, .{
        .flags = flags,
        .sq_thread_idle = 1000,
        .wq_fd = @as(u32, @bitCast(self.ring.fd)),
    });

    return .{ .ring = try .init_params(entries, &params) };
}

pub fn done(self: *Uring) bool {
    return self.in_flight.empty();
}

/// Return a file descriptor which can be used to poll the ring for completions
pub fn pollableFd(self: *Uring) !posix.fd_t {
    if (self.eventfd) |fd| return fd;
    const fd: posix.fd_t = @intCast(linux.eventfd(0, linux.EFD.CLOEXEC | linux.EFD.NONBLOCK));
    try self.ring.register_eventfd(fd);
    self.eventfd = fd;
    return fd;
}

pub fn submitAndWait(self: *Uring, queue: *io.SubmissionQueue) !void {
    var sqes_available = self.sqesAvailable();
    while (queue.pop()) |task| {
        const sqes_required = sqesRequired(task);
        if (sqes_available < sqes_required) {
            sqes_available += try self.ring.submit();
            continue;
        }
        defer sqes_available -= sqes_required;
        self.prepTask(task);
    }

    while (true) {
        _ = self.ring.submit_and_wait(1) catch |err| {
            switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            }
        };
        return;
    }
}

pub fn submit(self: *Uring, queue: *io.SubmissionQueue) !void {
    var sqes_available = self.sqesAvailable();
    while (queue.pop()) |task| {
        const sqes_required = sqesRequired(task);
        if (sqes_available < sqes_required) {
            sqes_available += try self.ring.submit();
            continue;
        }
        defer sqes_available -= sqes_required;
        self.prepTask(task);
    }
    const n = try self.ring.submit();
    _ = try self.ring.enter(n, 0, linux.IORING_ENTER_GETEVENTS);
}

fn sqesRequired(task: *const io.Task) u32 {
    return if (task.deadline == null) 1 else 2;
}

fn sqesAvailable(self: *Uring) u32 {
    return @intCast(self.ring.sq.sqes.len - self.ring.sq_ready());
}

fn prepTask(self: *Uring, task: *io.Task) void {
    self.in_flight.push(task);
    switch (task.req) {
        .noop => {
            const sqe = self.getSqe();
            sqe.prep_nop();
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        // Deadlines are always prepared from their parent task
        .deadline => unreachable,

        .timer => |*t| {
            const sqe = self.getSqe();
            sqe.prep_timeout(@ptrCast(t), 0, linux.IORING_TIMEOUT_REALTIME);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .cancel => |c| {
            const sqe = self.getSqe();
            switch (c) {
                .all => sqe.prep_cancel(0, linux.IORING_ASYNC_CANCEL_ANY),
                .task => |t| sqe.prep_cancel(@intFromPtr(t), 0),
            }
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .accept => |fd| {
            const sqe = self.getSqe();
            sqe.prep_multishot_accept(fd, null, null, 0);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .msg_ring => |msg| {
            const sqe = self.getSqe();
            const fd = msg.target.backend.platform.ring.fd;
            sqe.prep_rw(.MSG_RING, fd, 0, 0, @intFromPtr(msg.task));
            sqe.user_data = @intFromPtr(task);
            // Pass flags on the sent CQE. We use this to distinguish between a received message and
            // a message freom our own loop
            sqe.rw_flags |= linux.IORING_MSG_RING_FLAGS_PASS;
            sqe.splice_fd_in |= msg_ring_received_cqe;
            self.prepDeadline(task, sqe);
        },

        .recv => |req| {
            const sqe = self.getSqe();
            sqe.prep_recv(req.fd, req.buffer, 0);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .write => |req| {
            const sqe = self.getSqe();
            sqe.prep_write(req.fd, req.buffer, 0);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .writev => |req| {
            const sqe = self.getSqe();
            sqe.prep_writev(req.fd, req.vecs, 0);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .close => |fd| {
            const sqe = self.getSqe();
            sqe.prep_close(fd);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .poll => |req| {
            const sqe = self.getSqe();
            sqe.prep_poll_add(req.fd, req.mask);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .socket => |req| {
            const sqe = self.getSqe();
            sqe.prep_socket(req.domain, req.type, req.protocol, 0);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        .connect => |req| {
            const sqe = self.getSqe();
            sqe.prep_connect(req.fd, req.addr, req.addr_len);
            sqe.user_data = @intFromPtr(task);
            self.prepDeadline(task, sqe);
        },

        // user* is only sent internally between rings and higher level wrappers
        .userfd, .usermsg, .userptr => unreachable,
    }
}

fn prepDeadline(self: *Uring, parent_task: *io.Task, parent_sqe: *linux.io_uring_sqe) void {
    const task = parent_task.deadline orelse return;
    self.in_flight.push(task);
    assert(task.req == .deadline);
    parent_sqe.flags |= linux.IOSQE_IO_LINK;

    const sqe = self.getSqe();
    const flags = linux.IORING_TIMEOUT_ABS | // absolute time
        linux.IORING_TIMEOUT_REALTIME; // use the realtime clock (as opposed to boot time)
    sqe.prep_link_timeout(@ptrCast(&task.req.deadline), flags);
    sqe.user_data = @intFromPtr(task);
}

/// Get an sqe from the ring. Caller should only call this function if they are sure we have an SQE
/// available. Asserts that we have one available
fn getSqe(self: *Uring) *linux.io_uring_sqe {
    assert(self.ring.sq.sqes.len > self.ring.sq_ready());
    return self.ring.get_sqe() catch unreachable;
}

pub fn reapCompletions(self: *Uring, rt: *io.Ring) anyerror!void {
    var cqes: [64]linux.io_uring_cqe = undefined;
    const n = self.ring.copy_cqes(&cqes, 0) catch |err| {
        switch (err) {
            error.SignalInterrupt => return,
            else => return err,
        }
    };
    for (cqes[0..n]) |cqe| {
        const task: *io.Task = @ptrFromInt(cqe.user_data);

        task.result = switch (task.req) {
            .noop => .noop,

            // Deadlines we don't do anything for, these are always sent to a noopCallback
            .deadline => .{ .deadline = {} },

            .timer => .{ .timer = switch (cqeToE(cqe.res)) {
                .SUCCESS, .TIME => {},
                .INVAL, .FAULT => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .cancel => .{ .cancel = switch (cqeToE(cqe.res)) {
                .SUCCESS => {},
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                .NOENT => io.CancelError.EntryNotFound,
                .ALREADY => io.CancelError.NotCanceled,
                else => |e| unexpectedError(e),
            } },

            .accept => .{ .accept = switch (cqeToE(cqe.res)) {
                .SUCCESS => cqe.res,
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .msg_ring => .{ .msg_ring = switch (cqeToE(cqe.res)) {
                .SUCCESS => {},
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .recv => .{ .recv = switch (cqeToE(cqe.res)) {
                .SUCCESS => @intCast(cqe.res),
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                .CONNRESET => io.RecvError.ConnectionResetByPeer,
                else => |e| unexpectedError(e),
            } },

            .write => .{ .write = switch (cqeToE(cqe.res)) {
                .SUCCESS => @intCast(cqe.res),
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .writev => .{ .writev = switch (cqeToE(cqe.res)) {
                .SUCCESS => @intCast(cqe.res),
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .close => .{ .close = switch (cqeToE(cqe.res)) {
                .SUCCESS => {},
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .poll => .{ .poll = switch (cqeToE(cqe.res)) {
                .SUCCESS => {},
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .socket => .{ .socket = switch (cqeToE(cqe.res)) {
                .SUCCESS => @intCast(cqe.res),
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .connect => .{ .connect = switch (cqeToE(cqe.res)) {
                .SUCCESS => {},
                .INVAL => io.ResultError.Invalid,
                .CANCELED => io.ResultError.Canceled,
                else => |e| unexpectedError(e),
            } },

            .usermsg => .{ .usermsg = @intCast(cqe.res) },

            // userfd should never reach the runtime
            .userfd, .userptr => unreachable,
        };

        defer {
            if (cqe.flags & msg_ring_received_cqe != 0) {
                // This message was received from another ring. We don't decrement inflight for this.
                // But we do need to set the task as free because we will add it to our free list
                rt.free_q.push(task);
            } else if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
                // If the cqe doesn't have IORING_CQE_F_MORE set, then this task is complete and free to
                // be rescheduled
                task.state = .complete;
                self.in_flight.remove(task);
                rt.free_q.push(task);
            }
        }

        try task.callback(rt, task.*);
    }
}

fn cqeToE(result: i32) std.posix.E {
    if (result > -4096 and result < 0) {
        return @as(std.posix.E, @enumFromInt(-result));
    }
    return .SUCCESS;
}

fn unexpectedError(err: posix.E) posix.UnexpectedError {
    std.log.err("unexpected posix error: {}", .{err});
    return error.Unexpected;
}
