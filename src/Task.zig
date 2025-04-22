const Task = @This();

const std = @import("std");
const io = @import("main.zig");

const Allocator = std.mem.Allocator;
const Runtime = io.Runtime;

userdata: ?*anyopaque = null,
msg: u16 = 0,
callback: io.Callback = io.noopCallback,
req: io.Request = .noop,

result: ?io.Result = null,

state: enum {
    /// The task is free to be scheduled
    free,

    /// The task is in flight and may not be rescheduled. Some operations generate multiple
    /// completions, so it is possible to receive a task in Callback and the task is still
    /// considered to be in flight
    in_flight,

    /// The task was completed
    complete,

    /// The operation was canceled
    canceled,
} = .free,

/// Deadline for the task to complete, in absolute time. If 0, there is no deadline
deadline: ?*Task = null,

next: ?*Task = null,
prev: ?*Task = null,

pub fn setDeadline(
    self: *Task,
    rt: *Runtime,
    deadline: io.Timespec,
) Allocator.Error!void {
    std.debug.assert(!deadline.isZero());
    const task = try rt.getTask();

    task.* = .{
        .callback = io.noopCallback,
        .userdata = null,
        .msg = 0,
        .req = .{ .deadline = deadline },
    };

    self.deadline = task;
}

pub fn cancel(
    self: *Task,
    rt: *Runtime,
    ctx: io.Context,
) Allocator.Error!void {
    const task = try rt.getTask();
    task.* = .{
        .callback = ctx.cb,
        .msg = ctx.msg,
        .userdata = ctx.ptr,
        .req = .{ .cancel = .{ .task = self } },
    };
    rt.submission_q.push(task);
}

pub fn userdataCast(self: Task, comptime T: type) *T {
    return @ptrCast(@alignCast(self.userdata));
}

pub fn msgToEnum(self: Task, comptime Enum: type) Enum {
    return @enumFromInt(self.msg);
}
