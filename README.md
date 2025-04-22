# Ourio

<p align="center">
  <img width="128" height="128" src="ouroboros.svg">
</p>

Ourio (prounounced "oreo", think "Ouroboros") is an asynchronous IO runtime
built heavily around the semantics of io_uring. The design is inspired by
[libxev](https://github.com/mitchellh/libxev), which is in turn inspired by
[TigerBeetle](https://github.com/tigerbeetle/tigerbeetle).

Ourio has only a slightly different approach: it is designed to encourage
message passing approach to asynchronous IO. Users of the library give each task
a Context, which contains a pointer, a callback, *and a message*. The message is
implemented as a u16, and generally you should use an enum for it. The idea is
that you can minimize the number of callback functions required by tagging tasks
with a small amount of semantic meaning in the `msg` field.

Ourio has io_uring and kqueue backends. Ourio supports the `msg_ring`
capability of io_uring to pass a completion from one ring to another. This
allows a multithreaded application to implement message passing using io_uring
(or kqueue, if that's your flavor). Multithreaded applications should plan to
use one `Ring` per thread. Submission onto the runtime is not thread safe,
any message passing must occur using `msg_ring` rather than directly submitting
a task to another

Ourio also includes a fully mockable IO runtime to make it easy to unit test
your async code.

## Tasks

### Deadlines and Cancelation

Each IO operation creates a `Task`. When scheduling a task on the runtime, the
caller receives a pointer to the `Task` at which point they could cancel it, or
set a deadline.

```zig
// Timers are always relative time
const task = try rt.timer(.{.sec = 3}, .{.cb = onCompletion, .msg = 0});

// If the deadline expired, the task will be sent to the onCompletion callback
// with a result of error.Canceled. Deadlines are always absolute time
try task.setDeadline(rt, .{.sec = std.time.timestamp() + 3});

// Alternatively, we can hold on to the pointer for the task while it is with
// the runtime and cancel it. The Context we give to the cancel function let's
// us know the result of the cancelation, but we will also receive a message
// from the original task with error.Canceled. We can ignore the cancel result
// by using the default context value
try task.cancel(rt, .{});
```

### Passing tasks between threads

Say we `accept` a connection in one thread, and want to send the file descriptor
to another for handling.

```zig
const target_task = try main_rt.getTask();
target_task.* {
    .userdata = &foo,
    .msg = @intFromEnum(Msg.some_message),
    .cb = Worker.onCompletion,
    .req = .{ .userfd = fd },
};

// Send target_task from the main_rt thread to the thread_rt Ring. The
// thread_rt Ring will then // process the task as a completion, ie
// Worker.onCompletion will be called with // this task. That thread can then
// schedule a recv, a write, etc on the file // descriptor it just received.
_ = try main_rt.msgRing(thread_rt, target_task, .{});
```

### Multiple Rings on the same thread

You can have multiple Rings in a single thread. One could be a priority
Ring, or handle specific types of tasks, etc. Poll any `Ring` from any other
`Ring`.

```zig
const fd = rt1.backend.pollableFd();
_ = try rt2.poll(fd, .{
    .cb = onCompletion, 
    .msg = @intFromEnum(Msg.rt1_has_completions)}
);
```

## Example

An example implementation of an asynchronous writer to two file descriptors:

```zig
const std = @import("std");
const io = @import("ourio");
const posix = std.posix;

pub const MultiWriter = struct {
    fd1: posix.fd_t,
    fd1_written: usize = 0,

    fd2: posix.fd_2,
    fd2_written: usize = 0,

    buf: std.ArrayListUnmanaged(u8),

    pub const Msg = enum { fd1, fd2 };

    pub fn init(fd1: posix.fd_t, fd2: posix.fd_t) MultiWriter {
        return .{ .fd1 = fd1, .fd2 = fd2 };
    }

    pub fn write(self: *MultiWriter, gpa: Allocator, bytes: []const u8) !void {
        try self.buf.appendSlice(gpa, bytes);
    }

    pub fn flush(self: *MultiWriter, rt: *io.Ring) !void {
        if (self.fd1_written < self.buf.items.len) {
            _ = try rt.write(self.fd1, self.buf.items[self.fd1_written..], .{
                .ptr = self,
                .msg = @intFromEnum(Msg.fd1),
                .cb = MultiWriter.onCompletion,
            });
        }

        if (self.fd2_written < self.buf.items.len) {
            _ = try rt.write(self.fd2, self.buf.items[self.fd2_written..], .{
                .ptr = self,
                .msg = @intFromEnum(Msg.fd2),
                .cb = MultiWriter.onCompletion,
            });
        }
    }

    pub fn onCompletion(rt: *io.Ring, task: io.Task) anyerror!void {
        const self = task.userdataCast(MultiWriter);
        const result = task.result.?;

        const n = try result.write;
        switch (task.msgToEnum(MultiWriter.Msg)) {
            .fd1 => self.fd1_written += n,
            .fd2 => self.fd2_written += n,
        }

        const len = self.buf.items.len;

        if (self.fd1_written < len or self.fd2_written < len) 
	    return self.flush(rt);

        self.fd1_written = 0;
        self.fd2_written = 0;
        self.buf.clearRetainingCapacity();
    }
};

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    var rt: io.Ring = try .init(gpa.allocator(), 16);
    defer rt.deinit();

    // Pretend I created some files
    const fd1: posix.fd_t = 5;
    const fd2: posix.fd_t = 6;

    var mw: MultiWriter = .init(fd1, fd2);
    try mw.write(gpa.allocator(), "Hello, world!");
    try mw.flush(&rt);

    try rt.run(.until_done);
}
```
