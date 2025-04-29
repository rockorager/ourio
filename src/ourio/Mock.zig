const Mock = @This();

const std = @import("std");

const io = @import("../ourio.zig");

const Allocator = std.mem.Allocator;
const Queue = @import("queue.zig").Intrusive;
const assert = std.debug.assert;
const posix = std.posix;

completions: Queue(io.Task, .complete) = .{},

accept_cb: ?*const fn (*io.Task) io.Result = null,
cancel_cb: ?*const fn (*io.Task) io.Result = null,
close_cb: ?*const fn (*io.Task) io.Result = null,
connect_cb: ?*const fn (*io.Task) io.Result = null,
deadline_cb: ?*const fn (*io.Task) io.Result = null,
msg_ring_cb: ?*const fn (*io.Task) io.Result = null,
noop_cb: ?*const fn (*io.Task) io.Result = null,
open_cb: ?*const fn (*io.Task) io.Result = null,
poll_cb: ?*const fn (*io.Task) io.Result = null,
readv_cb: ?*const fn (*io.Task) io.Result = null,
recv_cb: ?*const fn (*io.Task) io.Result = null,
socket_cb: ?*const fn (*io.Task) io.Result = null,
statx_cb: ?*const fn (*io.Task) io.Result = null,
timer_cb: ?*const fn (*io.Task) io.Result = null,
write_cb: ?*const fn (*io.Task) io.Result = null,
writev_cb: ?*const fn (*io.Task) io.Result = null,

userfd_cb: ?*const fn (*io.Task) io.Result = null,
usermsg_cb: ?*const fn (*io.Task) io.Result = null,
userptr_cb: ?*const fn (*io.Task) io.Result = null,

/// Initialize a Ring
pub fn init(_: u16) !Mock {
    return .{};
}

pub fn deinit(self: *Mock, _: Allocator) void {
    self.* = undefined;
}

pub fn done(self: *Mock) bool {
    return self.completions.empty();
}

/// Initializes a child Ring which can be woken up by self. This must be called from the thread
/// which will operate the child ring. Initializes with the same queue size as the parent
pub fn initChild(_: Mock, entries: u16) !Mock {
    return init(entries);
}

/// Return a file descriptor which can be used to poll the ring for completions
pub fn pollableFd(_: *Mock) !posix.fd_t {
    return -1;
}

pub fn submitAndWait(self: *Mock, queue: *Queue(io.Task, .in_flight)) !void {
    return self.submit(queue);
}

pub fn submit(self: *Mock, queue: *Queue(io.Task, .in_flight)) !void {
    while (queue.pop()) |task| {
        task.result = switch (task.req) {
            .accept => if (self.accept_cb) |cb| cb(task) else return error.NoMockCallback,
            .cancel => if (self.cancel_cb) |cb| cb(task) else return error.NoMockCallback,
            .close => if (self.close_cb) |cb| cb(task) else return error.NoMockCallback,
            .connect => if (self.connect_cb) |cb| cb(task) else return error.NoMockCallback,
            .deadline => if (self.deadline_cb) |cb| cb(task) else return error.NoMockCallback,
            .msg_ring => if (self.msg_ring_cb) |cb| cb(task) else return error.NoMockCallback,
            .noop => if (self.noop_cb) |cb| cb(task) else return error.NoMockCallback,
            .open => if (self.open_cb) |cb| cb(task) else return error.NoMockCallback,
            .poll => if (self.poll_cb) |cb| cb(task) else return error.NoMockCallback,
            .readv => if (self.readv_cb) |cb| cb(task) else return error.NoMockCallback,
            .recv => if (self.recv_cb) |cb| cb(task) else return error.NoMockCallback,
            .socket => if (self.socket_cb) |cb| cb(task) else return error.NoMockCallback,
            .statx => if (self.statx_cb) |cb| cb(task) else return error.NoMockCallback,
            .timer => if (self.timer_cb) |cb| cb(task) else return error.NoMockCallback,
            .userfd => if (self.userfd_cb) |cb| cb(task) else return error.NoMockCallback,
            .usermsg => if (self.usermsg_cb) |cb| cb(task) else return error.NoMockCallback,
            .userptr => if (self.userptr_cb) |cb| cb(task) else return error.NoMockCallback,
            .write => if (self.write_cb) |cb| cb(task) else return error.NoMockCallback,
            .writev => if (self.writev_cb) |cb| cb(task) else return error.NoMockCallback,
        };
        self.completions.push(task);
    }
}

pub fn reapCompletions(self: *Mock, rt: *io.Ring) anyerror!void {
    while (self.completions.pop()) |task| {
        try task.callback(rt, task.*);
        rt.free_q.push(task);
    }
}
