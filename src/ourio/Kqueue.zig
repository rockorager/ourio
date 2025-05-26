const Kqueue = @This();

const std = @import("std");
const builtin = @import("builtin");

const io = @import("../ourio.zig");

const Allocator = std.mem.Allocator;
const EV = std.c.EV;
const EVFILT = std.c.EVFILT;
const Queue = @import("queue.zig").Intrusive;
const assert = std.debug.assert;
const posix = std.posix;

gpa: Allocator,
kq: posix.fd_t,
/// Items we have prepared and waiting to be put into kqueue
submission_queue: std.ArrayListUnmanaged(posix.Kevent) = .empty,

/// Tasks that have been submitted to kqueue
in_flight: Queue(io.Task, .in_flight) = .{},

/// Tasks which were completed synchronously during submission
synchronous_queue: Queue(io.Task, .complete) = .{},

/// Queue for other kqueue instances to send "completion" tasks to this thread
msg_ring_queue: Queue(io.Task, .complete) = .{},

/// Mutex to guard access to the msg_ring_queue. We *could* merge these tasks with the
/// synchronous_queue, however we want to limit contention and msg_ring is probably pretty rare
/// compared to synchronous tasks
msg_ring_mutex: std.Thread.Mutex = .{},

/// List of timers, sorted descending so when we pop we get the next timer to expire
timers: std.ArrayListUnmanaged(Timer) = .empty,

events: [128]posix.Kevent = undefined,
event_idx: usize = 0,

const Timer = union(enum) {
    /// a deadline timer cancels a task if it fires
    deadline: struct {
        /// the deadline task. If the parent completes before the deadline, the parent will set the
        /// deadline task state to .free
        task: *io.Task,

        /// the task to cancel if the deadline expires
        parent: *io.Task,
    },

    /// a regular timer
    timeout: struct {
        /// The task for the timer
        task: *io.Task,

        /// Absolute time in ms the timer was added
        added_ms: i64,
    },

    /// Timer expires in the return value milliseconds from now
    fn expiresInMs(self: Timer, now_abs: i64) i64 {
        switch (self) {
            .deadline => |deadline| {
                const ts = deadline.task.req.deadline;
                const expires_ms = ts.sec * std.time.ms_per_s + @divTrunc(ts.nsec, std.time.ns_per_ms);
                return expires_ms - now_abs;
            },

            // timeouts are relative, so we add the time it was added to the queue
            .timeout => |timeout| {
                const ts = timeout.task.req.timer;
                const expires_ms = ts.sec * std.time.ms_per_s +
                    @divTrunc(ts.nsec, std.time.ns_per_ms) +
                    timeout.added_ms;
                return expires_ms - now_abs;
            },
        }
    }

    /// returns a timespec suitable for a kevent timeout. Relative to now
    fn timespec(self: Timer) posix.timespec {
        const expires = self.expiresInMs(std.time.milliTimestamp());
        return .{ .sec = @divFloor(expires, 1000), .nsec = @mod(expires, 1000) * std.time.ns_per_ms };
    }

    fn lessThan(now_ms: i64, lhs: Timer, rhs: Timer) bool {
        // reverse sort (we want soonest expiring last)
        return lhs.expiresInMs(now_ms) > rhs.expiresInMs(now_ms);
    }
};

/// Messages we are waiting on using an EVFILT.USER
const UserMsg = enum {
    /// A general wakeup message
    wakeup,

    fn fromInt(v: i64) UserMsg {
        return @enumFromInt(v);
    }
};

/// Initialize a Ring
pub fn init(gpa: Allocator, _: u16) !Kqueue {
    const kq = try posix.kqueue();

    // Register a wakeup EVFILT.USER to wake up this kqueue
    var kevent = evSet(
        @intFromEnum(UserMsg.wakeup),
        EVFILT.USER,
        EV.ADD | EV.CLEAR,
        null,
    );
    kevent.fflags = std.c.NOTE.FFNOP;
    _ = try posix.kevent(kq, &.{kevent}, &.{}, null);

    return .{ .gpa = gpa, .kq = kq };
}

pub fn deinit(self: *Kqueue, gpa: Allocator) void {
    while (self.msg_ring_queue.pop()) |task| gpa.destroy(task);
    while (self.in_flight.pop()) |task| gpa.destroy(task);

    self.submission_queue.deinit(gpa);
    self.timers.deinit(gpa);

    posix.close(self.kq);
    self.* = undefined;
}

/// Initializes a child Ring which can be woken up by self. This must be called from the thread
/// which will operate the child ring. Initializes with the same queue size as the parent
pub fn initChild(self: Kqueue, entries: u16) !Kqueue {
    return init(self.gpa, entries);
}

pub fn submitAndWait(self: *Kqueue, queue: *io.SubmissionQueue) !void {
    defer self.submission_queue.clearRetainingCapacity();
    while (queue.pop()) |task| {
        try self.prepTask(task);

        // If this task is queued and has a deadline we need to schedule a timer
        if (task.deadline) |deadline| {
            try self.addTimer(.{ .deadline = .{ .task = deadline, .parent = task } });
        }
    }

    // Sort our timers
    const now = std.time.milliTimestamp();
    std.sort.insertion(Timer, self.timers.items, now, Timer.lessThan);

    if (self.synchronous_queue.empty()) {
        // We don't have any synchronous completions, so we need to wait for some from kqueue
        return self.wait();
    }

    // We already have completions from synchronous tasks. Submit our queued events and grab any new
    // completions for processing. We do so with a 0 timeout so that we are only grabbing already
    // completed items
    const timeout: posix.timespec = .{ .sec = 0, .nsec = 0 };
    self.event_idx = try posix.kevent(self.kq, self.submission_queue.items, &self.events, &timeout);
}

fn wait(self: *Kqueue) !void {
    assert(self.synchronous_queue.empty());

    // Go through our times until the first unexpired one
    while (true) {
        const t = self.timers.getLastOrNull() orelse break;
        const timeout: posix.timespec = t.timespec();

        self.event_idx = try posix.kevent(
            self.kq,
            self.submission_queue.items,
            &self.events,
            &timeout,
        );
        return;
    }

    // We had no timers so we wait indefinitely
    self.event_idx = try posix.kevent(
        self.kq,
        self.submission_queue.items,
        &self.events,
        null,
    );
}

pub fn submit(self: *Kqueue, queue: *io.SubmissionQueue) !void {
    defer self.submission_queue.clearRetainingCapacity();
    while (queue.pop()) |task| {
        try self.prepTask(task);

        // If this task is queued and has a deadline we need to schedule a timer
        if (task.state == .in_flight and task.deadline != null) {
            const deadline = task.deadline.?;
            try self.addTimer(.{ .deadline = .{ .task = deadline, .parent = task } });
        }
    }

    // Sort our timers
    const now = std.time.milliTimestamp();
    std.sort.insertion(Timer, self.timers.items, now, Timer.lessThan);

    // For submit, we don't try to reap any completions. Calls to submit will likely be relying on a
    // `poll` of the kqueue. We check in reapCompletinos if we have no reaped events and grab them
    // there if needed
    const timeout: posix.timespec = .{ .sec = 0, .nsec = 0 };
    _ = try posix.kevent(self.kq, self.submission_queue.items, &.{}, &timeout);
}

/// preps a task to be submitted into the kqueue
fn prepTask(self: *Kqueue, task: *io.Task) !void {
    return switch (task.req) {
        .accept => |req| {
            self.in_flight.push(task);
            const kevent = evSet(@intCast(req.fd), EVFILT.READ, EV.ADD, task);
            try self.submission_queue.append(self.gpa, kevent);
        },

        .cancel => |req| {
            // Cancel tasks are handled like this:
            // 0. If the task is not in flight, we don't do anything
            // 1. We set the cancel_task state as canceled
            // 2. If there is a kqueue event associated with it, we prep a kevent with EV.DELETE
            // 3. We add the task to the synchronous queue. This let's us ensure we hold the
            //    cancel state until we've submitted the kevent associated with it. If any
            //    completions occur while the state is canceled, we will ignore them.
            // 4. Callbacks are not called in the submit phase. We only call the callback of the
            //    cancel_task with error.Canceled in reapCompletions
            // 5. In reapCompletions, we will return both the task and the cancel_task to the free
            //    list

            self.synchronous_queue.push(task);

            task.result = .{ .cancel = {} };
            switch (req) {
                .all => {
                    while (self.in_flight.head) |t| {
                        defer self.synchronous_queue.push(t);

                        self.cancelTask(t) catch continue;
                    }

                    while (self.timers.getLastOrNull()) |tmr| {
                        switch (tmr) {
                            inline else => |v| {
                                defer self.synchronous_queue.push(v.task);
                                self.cancelTask(v.task) catch continue;
                            },
                        }
                    }
                },
                .task => |task_to_cancel| {
                    switch (task_to_cancel.state) {
                        .free, .canceled, .complete => {
                            task.result = .{ .cancel = error.NotCanceled };
                            return;
                        },
                        .in_flight => {},
                    }
                    try self.cancelTask(task_to_cancel);
                },
            }
        },

        .close => |req| {
            task.result = .{ .close = {} };
            self.synchronous_queue.push(task);
            posix.close(req);
        },

        .connect => |req| {
            // Set nonblocking. Call connect. Then add it to the kqueue. This will return as
            // writeable when the connect is complete
            const arg: posix.O = .{ .NONBLOCK = true };
            const arg_u32: u32 = @bitCast(arg);
            _ = posix.fcntl(req.fd, posix.F.SETFL, arg_u32) catch {
                task.result = .{ .connect = error.Unexpected };
                self.synchronous_queue.push(task);
                return;
            };

            if (posix.connect(req.fd, req.addr, req.addr_len)) {
                // We connected immediately. No need to add to kqueue. Just push to the synchronous
                // queue to call the callback later
                task.result = .{ .connect = {} };
                self.synchronous_queue.push(task);
            } else |err| {
                switch (err) {
                    error.WouldBlock => {
                        self.in_flight.push(task);
                        // This is the error we expect. Add the event to kqueue
                        const kevent = evSet(@intCast(req.fd), EVFILT.WRITE, EV.ADD | EV.ONESHOT, task);
                        try self.submission_queue.append(self.gpa, kevent);
                    },
                    else => {
                        task.result = .{ .connect = error.Unexpected };
                        self.synchronous_queue.push(task);
                    },
                }
            }
        },

        // deadlines are handled separately
        .deadline => unreachable,

        .msg_ring => |req| {
            const target = req.target;

            target.backend.platform.msg_ring_mutex.lock();
            target.backend.platform.msg_ring_queue.push(req.task);
            target.backend.platform.msg_ring_mutex.unlock();

            task.result = .{ .msg_ring = {} };
            self.synchronous_queue.push(task);

            // wake up the other ring
            var kevent = evSet(
                @intFromEnum(UserMsg.wakeup),
                EVFILT.USER,
                0,
                null,
            );
            kevent.fflags |= std.c.NOTE.TRIGGER;
            // Trigger the wakeup
            _ = try posix.kevent(target.backend.platform.kq, &.{kevent}, &.{}, null);
        },

        .noop => {
            task.result = .noop;
            self.synchronous_queue.push(task);
        },

        .open => |req| {
            self.synchronous_queue.push(task);
            const rc = posix.open(req.path, req.flags, req.mode);
            task.result = .{ .open = rc };
        },

        .poll => |req| {
            self.in_flight.push(task);
            if (req.mask & posix.POLL.IN != 0) {
                const kevent = evSet(@intCast(req.fd), EVFILT.READ, EV.ADD | EV.ONESHOT, task);
                try self.submission_queue.append(self.gpa, kevent);
            }
            if (req.mask & posix.POLL.OUT != 0) {
                const kevent = evSet(@intCast(req.fd), EVFILT.WRITE, EV.ADD | EV.ONESHOT, task);
                try self.submission_queue.append(self.gpa, kevent);
            }
        },

        .read => |req| {
            self.in_flight.push(task);
            const kevent = evSet(@intCast(req.fd), EVFILT.READ, EV.ADD | EV.ONESHOT, task);
            try self.submission_queue.append(self.gpa, kevent);
        },

        .readv => |req| {
            self.in_flight.push(task);
            const kevent = evSet(@intCast(req.fd), EVFILT.READ, EV.ADD | EV.ONESHOT, task);
            try self.submission_queue.append(self.gpa, kevent);
        },

        .recv => |req| {
            self.in_flight.push(task);
            const kevent = evSet(@intCast(req.fd), EVFILT.READ, EV.ADD | EV.ONESHOT, task);
            try self.submission_queue.append(self.gpa, kevent);
        },

        .socket => |req| {
            self.synchronous_queue.push(task);
            if (posix.socket(req.domain, req.type, req.protocol)) |fd|
                task.result = .{ .socket = fd }
            else |_|
                task.result = .{ .socket = error.Unexpected };
        },

        .splice => |req| {
            self.in_flight.push(task);
            const kevent = evSet(@intCast(req.fd_in), EVFILT.READ, EV.ADD | EV.ONESHOT, task);
            try self.submission_queue.append(self.gpa, kevent);
        },

        .statx => |*req| {
            self.synchronous_queue.push(task);
            const flags: u32 = if (req.symlink_follow) 0 else posix.AT.SYMLINK_NOFOLLOW;

            if (posix.fstatat(posix.AT.FDCWD, req.path, flags)) |stat| {
                req.result.* = .{
                    .mask = 0,
                    .blksize = @intCast(stat.blksize),
                    .attributes = 0,
                    .nlink = @intCast(stat.nlink),
                    .uid = @intCast(stat.uid),
                    .gid = @intCast(stat.gid),
                    .mode = @intCast(stat.mode),
                    .__pad1 = 0,
                    .ino = @intCast(stat.ino),
                    .size = @intCast(stat.size),
                    .blocks = @intCast(stat.blocks),
                    .attributes_mask = 0,
                    .atime = .{
                        .sec = @intCast(stat.atime().sec),
                        .nsec = @intCast(stat.atime().nsec),
                    },
                    .btime = .{
                        .sec = 0,
                        .nsec = 0,
                    },
                    .ctime = .{
                        .sec = @intCast(stat.ctime().sec),
                        .nsec = @intCast(stat.ctime().nsec),
                    },
                    .mtime = .{
                        .sec = @intCast(stat.mtime().sec),
                        .nsec = @intCast(stat.mtime().nsec),
                    },
                    .rdev_major = major(@intCast(stat.rdev)),
                    .rdev_minor = minor(@intCast(stat.rdev)),
                    .dev_major = major(@intCast(stat.dev)),
                    .dev_minor = major(@intCast(stat.dev)),
                    .__pad2 = undefined,
                };
                task.result = .{ .statx = req.result };
            } else |_| task.result = .{ .statx = error.Unexpected };
        },

        .timer => {
            const now = std.time.milliTimestamp();
            try self.addTimer(.{ .timeout = .{ .task = task, .added_ms = now } });
        },

        // user* fields are never seen by the runtime, only for internal message passing
        .userbytes, .userfd, .usermsg, .userptr => unreachable,

        .write => |req| {
            self.in_flight.push(task);
            const kevent = evSet(@intCast(req.fd), EVFILT.WRITE, EV.ADD | EV.ONESHOT, task);
            try self.submission_queue.append(self.gpa, kevent);
        },

        .writev => |req| {
            self.in_flight.push(task);
            const kevent = evSet(@intCast(req.fd), EVFILT.WRITE, EV.ADD | EV.ONESHOT, task);
            try self.submission_queue.append(self.gpa, kevent);
        },
    };
}

fn cancelTask(self: *Kqueue, task: *io.Task) !void {
    task.state = .canceled;
    if (task.deadline) |d| d.state = .canceled;

    switch (task.req) {
        // Handled synchronously. Probably we couldn't cancel it, but if we did somehow we
        // don't need to do anything either way
        .cancel,
        .close,
        .msg_ring,
        .noop,
        .open,
        .socket,
        .statx,
        .userbytes,
        .userfd,
        .usermsg,
        .userptr,
        => {},

        .accept => |cancel_req| {
            self.in_flight.remove(task);
            task.result = .{ .accept = error.Canceled };
            const kevent = evSet(@intCast(cancel_req.fd), EVFILT.READ, EV.DELETE, task);
            try self.submission_queue.append(self.gpa, kevent);
        },

        .connect => |cancel_req| {
            self.in_flight.remove(task);
            task.result = .{ .connect = error.Canceled };
            const kevent = evSet(
                @intCast(cancel_req.fd),
                EVFILT.WRITE,
                EV.DELETE,
                task,
            );
            try self.submission_queue.append(self.gpa, kevent);
        },

        .deadline => {
            // What does it mean to cancel a deadline? We remove the deadline from
            // the parent and the timer from our list
            for (self.timers.items, 0..) |t, i| {
                if (t == .deadline and t.deadline.task == task) {
                    // Set the parent deadline to null
                    t.deadline.parent.deadline = null;
                    // Remove the timer
                    _ = self.timers.orderedRemove(i);
                    task.result = .{ .deadline = error.Canceled };
                    return;
                }
            } else task.result = .{ .cancel = error.EntryNotFound };
        },

        .poll => |cancel_req| {
            self.in_flight.remove(task);
            task.result = .{ .poll = error.Canceled };
            if (cancel_req.mask & posix.POLL.IN != 0) {
                const kevent = evSet(@intCast(cancel_req.fd), EVFILT.READ, EV.DELETE, task);
                try self.submission_queue.append(self.gpa, kevent);
            }
            if (cancel_req.mask & posix.POLL.OUT != 0) {
                const kevent = evSet(@intCast(cancel_req.fd), EVFILT.WRITE, EV.DELETE, task);
                try self.submission_queue.append(self.gpa, kevent);
            }
        },

        .read => |cancel_req| {
            self.in_flight.remove(task);
            task.result = .{ .read = error.Canceled };
            const kevent = evSet(
                @intCast(cancel_req.fd),
                EVFILT.READ,
                EV.DELETE,
                task,
            );
            try self.submission_queue.append(self.gpa, kevent);
        },

        .readv => |cancel_req| {
            self.in_flight.remove(task);
            task.result = .{ .readv = error.Canceled };
            const kevent = evSet(
                @intCast(cancel_req.fd),
                EVFILT.READ,
                EV.DELETE,
                task,
            );
            try self.submission_queue.append(self.gpa, kevent);
        },

        .recv => |cancel_req| {
            self.in_flight.remove(task);
            task.result = .{ .recv = error.Canceled };
            const kevent = evSet(
                @intCast(cancel_req.fd),
                EVFILT.READ,
                EV.DELETE,
                task,
            );
            try self.submission_queue.append(self.gpa, kevent);
        },

        .splice => |cancel_req| {
            self.in_flight.remove(task);
            task.result = .{ .read = error.Canceled };
            const kevent = evSet(
                @intCast(cancel_req.fd_out),
                EVFILT.READ,
                EV.DELETE,
                task,
            );
            try self.submission_queue.append(self.gpa, kevent);
        },

        .timer => {
            for (self.timers.items, 0..) |t, i| {
                if (t == .timeout and t.timeout.task == task) {
                    // Remove the timer
                    _ = self.timers.orderedRemove(i);
                    task.result = .{ .timer = error.Canceled };
                    return;
                }
            } else task.result = .{ .cancel = error.EntryNotFound };
        },

        .write => |cancel_req| {
            self.in_flight.remove(task);
            task.result = .{ .write = error.Canceled };
            const kevent = evSet(
                @intCast(cancel_req.fd),
                EVFILT.WRITE,
                EV.DELETE,
                task,
            );
            try self.submission_queue.append(self.gpa, kevent);
        },

        .writev => |cancel_req| {
            self.in_flight.remove(task);
            task.result = .{ .writev = error.Canceled };
            const kevent = evSet(
                @intCast(cancel_req.fd),
                EVFILT.WRITE,
                EV.DELETE,
                task,
            );
            try self.submission_queue.append(self.gpa, kevent);
        },
    }
}

fn addTimer(self: *Kqueue, t: Timer) !void {
    try self.timers.append(self.gpa, t);
}

fn evSet(ident: usize, filter: i16, flags: u16, ptr: ?*anyopaque) posix.Kevent {
    return switch (builtin.os.tag) {
        .netbsd,
        .dragonfly,
        .openbsd,
        .macos,
        .ios,
        .tvos,
        .watchos,
        .visionos,
        => .{
            .ident = ident,
            .filter = filter,
            .flags = flags,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(ptr),
        },

        .freebsd => .{
            .ident = ident,
            .filter = filter,
            .flags = flags,
            .fflags = 0,
            .data = 0,
            .udata = @intFromPtr(ptr),
            ._ext = &.{ 0, 0, 0, 0 },
        },

        else => @compileError("kqueue not supported"),
    };
}

pub fn done(self: *Kqueue) bool {
    if (self.timers.items.len == 0 and
        self.in_flight.empty() and
        self.submission_queue.items.len == 0)
    {
        self.msg_ring_mutex.lock();
        defer self.msg_ring_mutex.unlock();
        return self.msg_ring_queue.empty();
    }

    return false;
}

/// Return a file descriptor which can be used to poll the ring for completions
pub fn pollableFd(self: Kqueue) !posix.fd_t {
    return self.kq;
}

pub fn reapCompletions(self: *Kqueue, rt: *io.Ring) anyerror!void {
    defer self.event_idx = 0;

    if (self.event_idx == 0) {
        const timeout: posix.timespec = .{ .sec = 0, .nsec = 0 };
        self.event_idx = try posix.kevent(
            self.kq,
            self.submission_queue.items,
            &self.events,
            &timeout,
        );
    }

    for (self.events[0..self.event_idx]) |event| {
        // if the event is a USER filter, we check our msg_ring_queue
        if (event.filter == EVFILT.USER) {
            switch (UserMsg.fromInt(event.data)) {
                .wakeup => {
                    // We got a message in our msg_ring_queue
                    self.msg_ring_mutex.lock();
                    defer self.msg_ring_mutex.unlock();

                    while (self.msg_ring_queue.pop()) |task| {
                        // For canceled msg_rings we do nothing
                        if (task.state == .canceled) continue;

                        defer self.releaseTask(rt, task);
                        if (task.result == null) task.result = .noop;
                        try task.callback(rt, task.*);
                    }
                },
            }
            continue;
        }

        const task: *io.Task = @ptrFromInt(event.udata);
        if (task.state == .canceled) continue;
        try self.handleCompletion(rt, task, event);
    }

    while (self.synchronous_queue.pop()) |task| {
        try self.handleSynchronousCompletion(rt, task);
    }

    const now = std.time.milliTimestamp();
    while (self.timers.getLastOrNull()) |t| {
        if (t.expiresInMs(now) > 0) break;
        _ = self.timers.pop();
        try self.handleExpiredTimer(rt, t);
    }
}

/// Handle a completion which was done synchronously. The work has already been done, we just need
/// to call the callback and return the task(s) to the free list
fn handleSynchronousCompletion(
    self: *Kqueue,
    rt: *io.Ring,
    task: *io.Task,
) !void {
    switch (task.req) {
        // async tasks. These can be handled synchronously in a cancel all
        .accept,
        .poll,
        .read,
        .readv,
        .recv,
        .splice,
        .write,
        .writev,

        // Timers can be handled synchronously in a cancel all
        .deadline,
        .timer,

        .connect, // connect is handled both sync and async
        .close,
        .msg_ring,
        .noop,
        .open,
        .socket,
        .statx,
        .userbytes,
        .userfd,
        .usermsg,
        .userptr,
        => {
            assert(task.result != null);
            defer self.releaseTask(rt, task);
            try task.callback(rt, task.*);
        },

        .cancel => |c| {
            assert(task.result != null);
            defer self.releaseTask(rt, task);
            try task.callback(rt, task.*);

            switch (c) {
                .all => {},

                .task => |ct| {
                    // If the cancel had an error, we don't need to return the task_to_cancel
                    _ = task.result.?.cancel catch return;
                    // On success, it is our job to call the canceled tasks' callback and return the
                    // task to the free list
                    defer self.releaseTask(rt, ct);
                    const result: io.Result = switch (ct.req) {
                        .accept => .{ .accept = error.Canceled },
                        .cancel => .{ .cancel = error.Canceled },
                        .close => .{ .close = error.Canceled },
                        .connect => .{ .connect = error.Canceled },
                        .deadline => .{ .deadline = error.Canceled },
                        .msg_ring => .{ .msg_ring = error.Canceled },
                        .noop => unreachable,
                        .open => .{ .open = error.Canceled },
                        .poll => .{ .poll = error.Canceled },
                        .read => .{ .read = error.Canceled },
                        .readv => .{ .readv = error.Canceled },
                        .recv => .{ .recv = error.Canceled },
                        .socket => .{ .socket = error.Canceled },
                        .splice => .{ .splice = error.Canceled },
                        .statx => .{ .statx = error.Canceled },
                        .timer => .{ .timer = error.Canceled },
                        .userbytes, .userfd, .usermsg, .userptr => unreachable,
                        .write => .{ .write = error.Canceled },
                        .writev => .{ .writev = error.Canceled },
                    };
                    ct.result = result;
                    try ct.callback(rt, ct.*);
                },
            }
        },
    }
}

fn dataToE(result: i64) std.posix.E {
    if (result > 0) {
        return @as(std.posix.E, @enumFromInt(-result));
    }
    return .SUCCESS;
}

fn unexpectedError(err: posix.E) posix.UnexpectedError {
    std.log.debug("unexpected posix error: {}", .{err});
    return error.Unexpected;
}

fn handleCompletion(
    self: *Kqueue,
    rt: *io.Ring,
    task: *io.Task,
    event: posix.Kevent,
) !void {
    switch (task.req) {
        .cancel,
        .close,
        .deadline,
        .msg_ring,
        .noop,
        .open,
        .socket,
        .statx,
        .timer,
        .userbytes,
        .userfd,
        .usermsg,
        .userptr,
        => unreachable,

        .accept => |req| {
            defer self.releaseTask(rt, task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                task.result = .{ .accept = err };
                return task.callback(rt, task.*);
            }

            if (posix.accept(req.fd, req.addr, req.addr_size, 0)) |fd|
                task.result = .{ .accept = fd }
            else |_|
                task.result = .{ .accept = error.Unexpected };
            return task.callback(rt, task.*);
        },

        .connect => {
            defer self.releaseTask(rt, task);
            self.in_flight.remove(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                task.result = .{ .connect = err };
            } else task.result = .{ .connect = {} };
            return task.callback(rt, task.*);
        },

        .poll => {
            defer self.releaseTask(rt, task);
            self.in_flight.remove(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                task.result = .{ .poll = err };
            } else task.result = .{ .poll = {} };
            return task.callback(rt, task.*);
        },

        .read => |req| {
            defer self.releaseTask(rt, task);
            self.in_flight.remove(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                task.result = .{ .read = err };
                return task.callback(rt, task.*);
            }
            if (posix.pread(req.fd, req.buffer, @intFromEnum(req.offset))) |n|
                task.result = .{ .read = n }
            else |_|
                task.result = .{ .read = error.Unexpected };
            return task.callback(rt, task.*);
        },

        .readv => |req| {
            defer self.releaseTask(rt, task);
            self.in_flight.remove(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                task.result = .{ .readv = err };
                return task.callback(rt, task.*);
            }
            if (posix.preadv(req.fd, req.vecs, @intFromEnum(req.offset))) |n|
                task.result = .{ .readv = n }
            else |_|
                task.result = .{ .readv = error.Unexpected };
            return task.callback(rt, task.*);
        },

        .recv => |req| {
            defer self.releaseTask(rt, task);
            self.in_flight.remove(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                task.result = .{ .recv = err };
                return task.callback(rt, task.*);
            }
            if (posix.recv(req.fd, req.buffer, 0)) |n|
                task.result = .{ .recv = n }
            else |_|
                task.result = .{ .recv = error.Unexpected };
            return task.callback(rt, task.*);
        },

        .splice => |req| {
            defer self.releaseTask(rt, task);
            self.in_flight.remove(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                task.result = .{ .splice = err };
                return task.callback(rt, task.*);
            }
            if (posix.sendfile(req.fd_out, req.fd_in, req.offset, req.nbytes, &.{}, &.{}, 0)) |n|
                task.result = .{ .splice = n }
            else |_|
                task.result = .{ .splice = error.Unexpected };
            return task.callback(rt, task.*);
        },

        .write => |req| {
            defer self.releaseTask(rt, task);
            self.in_flight.remove(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                task.result = .{ .write = err };
                return task.callback(rt, task.*);
            }
            if (posix.pwrite(req.fd, req.buffer, @intFromEnum(req.offset))) |n|
                task.result = .{ .write = n }
            else |_|
                task.result = .{ .write = error.Unexpected };
            return task.callback(rt, task.*);
        },

        .writev => |req| {
            defer self.releaseTask(rt, task);
            self.in_flight.remove(task);
            if (event.flags & EV.ERROR != 0) {
                // Interpret data as an errno
                const err = unexpectedError(dataToE(event.data));
                task.result = .{ .writev = err };
                return task.callback(rt, task.*);
            }
            if (posix.pwritev(req.fd, req.vecs, @intFromEnum(req.offset))) |n|
                task.result = .{ .writev = n }
            else |_|
                task.result = .{ .writev = error.Unexpected };
            return task.callback(rt, task.*);
        },
    }
}

fn releaseTask(self: *Kqueue, rt: *io.Ring, task: *io.Task) void {
    rt.free_q.push(task);
    if (task.deadline) |d| {
        // remove the deadline
        for (self.timers.items, 0..) |t, i| {
            if (t == .deadline and t.deadline.task == d) {
                // Remove the timer
                _ = self.timers.orderedRemove(i);
                rt.free_q.push(t.deadline.task);
                return;
            }
        }
    }
}

fn handleExpiredTimer(self: *Kqueue, rt: *io.Ring, t: Timer) !void {
    switch (t) {
        .deadline => |deadline| {
            defer self.releaseTask(rt, deadline.task);
            if (deadline.task.state == .canceled) return;

            try deadline.parent.cancel(rt, .{});
        },

        .timeout => |timeout| {
            const task = timeout.task;
            defer self.releaseTask(rt, task);
            if (task.state == .canceled) return;
            task.result = .{ .timer = {} };
            try task.callback(rt, task.*);
        },
    }
}
fn major(dev: u64) u32 {
    return switch (@import("builtin").target.os.tag) {
        .macos, .visionos, .tvos, .ios, .watchos => @intCast((dev >> 24) & 0xff),
        .freebsd, .openbsd, .netbsd, .dragonfly => @intCast((dev >> 8) & 0xff),
        else => @compileError("unsupported OS for major()"),
    };
}

fn minor(dev: u64) u32 {
    return switch (@import("builtin").target.os.tag) {
        .macos, .ios, .visionos, .tvos, .watchos => @intCast(dev & 0xffffff),
        .openbsd => @intCast(dev & 0xff),
        .freebsd => @intCast((dev & 0xff) | ((dev >> 12) & 0xfff00)),
        .dragonfly => @intCast((dev & 0xff) | ((dev >> 12) & 0xfff00)),
        .netbsd => @intCast((dev & 0xff) | ((dev >> 12) & 0xfff00)),
        else => @compileError("unsupported OS for minor()"),
    };
}
