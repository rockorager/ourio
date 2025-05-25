const std = @import("std");
const builtin = @import("builtin");

const Allocator = std.mem.Allocator;
pub const Mock = @import("ourio/Mock.zig");
const Queue = @import("ourio/queue.zig").Intrusive;
const io = @This();
const posix = std.posix;

pub const has_kqueue = switch (builtin.os.tag) {
    .dragonfly,
    .freebsd,
    .ios,
    .macos,
    .netbsd,
    .openbsd,
    .tvos,
    .visionos,
    .watchos,
    => true,

    else => false,
};
pub const has_io_uring = builtin.os.tag == .linux;

pub const Task = @import("ourio/Task.zig");
pub const Callback = *const fn (*Ring, Task) anyerror!void;
pub fn noopCallback(_: *Ring, _: Task) anyerror!void {}

pub const RunCondition = enum {
    once,
    until_done,
    forever,
};

pub const Context = struct {
    ptr: ?*anyopaque = null,
    msg: u16 = 0,
    cb: Callback = noopCallback,
};

/// Used for timeouts and deadlines. We make this struct extern because we will ptrCast it to the
/// linux kernel timespec struct
pub const Timespec = extern struct {
    sec: i64 = 0,
    nsec: i64 = 0,

    pub fn isZero(self: Timespec) bool {
        return self.sec == 0 and self.nsec == 0;
    }
};

pub const Backend = union(enum) {
    mock: Mock,

    platform: switch (builtin.os.tag) {
        .dragonfly,
        .freebsd,
        .ios,
        .macos,
        .netbsd,
        .openbsd,
        .tvos,
        .visionos,
        .watchos,
        => @import("ourio/Kqueue.zig"),

        .linux => @import("ourio/Uring.zig"),

        else => @compileError("unsupported os"),
    },

    pub fn initChild(self: *Backend, entries: u16) !Backend {
        switch (self.*) {
            .mock => return .{ .mock = .{} },
            .platform => |*p| return .{ .platform = try p.initChild(entries) },
        }
    }

    pub fn deinit(self: *Backend, gpa: Allocator) void {
        switch (self.*) {
            inline else => |*backend| backend.deinit(gpa),
        }
    }

    pub fn pollableFd(self: *Backend) !posix.fd_t {
        return switch (self.*) {
            inline else => |*backend| backend.pollableFd(),
        };
    }

    pub fn submitAndWait(self: *Backend, queue: *SubmissionQueue) !void {
        return switch (self.*) {
            inline else => |*backend| backend.submitAndWait(queue),
        };
    }

    pub fn submit(self: *Backend, queue: *SubmissionQueue) !void {
        return switch (self.*) {
            inline else => |*backend| backend.submit(queue),
        };
    }

    pub fn reapCompletions(
        self: *Backend,
        rt: *Ring,
    ) !void {
        return switch (self.*) {
            inline else => |*backend| backend.reapCompletions(rt),
        };
    }

    pub fn done(self: *Backend) bool {
        return switch (self.*) {
            inline else => |*backend| backend.done(),
        };
    }
};

pub const CompletionQueue = Queue(Task, .complete);
pub const FreeQueue = Queue(Task, .free);
pub const SubmissionQueue = Queue(Task, .in_flight);

pub const Ring = struct {
    backend: Backend,
    gpa: Allocator,

    completion_q: CompletionQueue = .{},
    submission_q: SubmissionQueue = .{},
    free_q: FreeQueue = .{},

    pub fn init(gpa: Allocator, entries: u16) !Ring {
        return .{
            .backend = .{ .platform = try .init(gpa, entries) },
            .gpa = gpa,
        };
    }

    pub fn initChild(self: *Ring, entries: u16) !Ring {
        return .{
            .backend = try self.backend.initChild(entries),
            .gpa = self.gpa,
        };
    }

    pub fn initMock(gpa: Allocator, entries: u16) !Ring {
        return .{
            .backend = .{ .mock = try .init(entries) },
            .gpa = gpa,
            .free_q = .{},
            .submission_q = .{},
            .completion_q = .{},
        };
    }

    pub fn deinit(self: *Ring) void {
        self.backend.deinit(self.gpa);
        while (self.free_q.pop()) |task| self.gpa.destroy(task);
        while (self.submission_q.pop()) |task| self.gpa.destroy(task);
        while (self.completion_q.pop()) |task| self.gpa.destroy(task);
    }

    pub fn run(self: *Ring, condition: RunCondition) !void {
        while (true) {
            try self.backend.submitAndWait(&self.submission_q);
            try self.backend.reapCompletions(self);
            switch (condition) {
                .once => return,
                .until_done => if (self.backend.done() and self.submission_q.empty()) return,
                .forever => {},
            }
        }
    }

    pub fn getTask(self: *Ring) Allocator.Error!*Task {
        return self.free_q.pop() orelse try self.gpa.create(Task);
    }

    pub fn noop(
        self: *Ring,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .noop,
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn timer(
        self: *Ring,
        duration: Timespec,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .timer = duration },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn cancelAll(self: *Ring) Allocator.Error!void {
        const task = try self.getTask();
        task.* = .{
            .req = .{ .cancel = .all },
        };

        self.submission_q.push(task);
    }

    pub fn accept(
        self: *Ring,
        fd: posix.fd_t,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .accept = fd },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn msgRing(
        self: *Ring,
        target: *Ring,
        target_task: *Task, // The task that the target ring will receive. The callbacks of
        // this task are what will be called when the target receives the message

        ctx: Context,
    ) Allocator.Error!*Task {
        // This is the task to send the message
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .msg_ring = .{
                .target = target,
                .task = target_task,
            } },
        };
        target_task.state = .in_flight;
        self.submission_q.push(task);
        return task;
    }

    pub fn recv(
        self: *Ring,
        fd: posix.fd_t,
        buffer: []u8,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .recv = .{
                .fd = fd,
                .buffer = buffer,
            } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn write(
        self: *Ring,
        fd: posix.fd_t,
        buffer: []const u8,
        offset: Offset,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .write = .{
                .fd = fd,
                .buffer = buffer,
                .offset = offset,
            } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn writev(
        self: *Ring,
        fd: posix.fd_t,
        vecs: []const posix.iovec_const,
        offset: Offset,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .writev = .{
                .fd = fd,
                .vecs = vecs,
                .offset = offset,
            } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn close(
        self: *Ring,
        fd: posix.fd_t,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .close = fd },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn poll(
        self: *Ring,
        fd: posix.fd_t,
        mask: u32,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .poll = .{ .fd = fd, .mask = mask } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn socket(
        self: *Ring,
        domain: u32,
        socket_type: u32,
        protocol: u32,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .socket = .{ .domain = domain, .type = socket_type, .protocol = protocol } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn connect(
        self: *Ring,
        fd: posix.socket_t,
        addr: *posix.sockaddr,
        addr_len: posix.socklen_t,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .connect = .{ .fd = fd, .addr = addr, .addr_len = addr_len } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn stat(
        self: *Ring,
        path: [:0]const u8,
        result: *Statx,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .statx = .{ .path = path, .result = result } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn lstat(
        self: *Ring,
        path: [:0]const u8,
        result: *Statx,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .statx = .{ .path = path, .result = result, .symlink_follow = false } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn readv(
        self: *Ring,
        fd: posix.fd_t,
        vecs: []const posix.iovec,
        offset: Offset,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .readv = .{
                .fd = fd,
                .vecs = vecs,
                .offset = offset,
            } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn read(
        self: *Ring,
        fd: posix.fd_t,
        buffer: []u8,
        offset: Offset,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .read = .{
                .fd = fd,
                .buffer = buffer,
                .offset = offset,
            } },
        };

        self.submission_q.push(task);
        return task;
    }

    pub fn open(
        self: *Ring,
        path: [:0]const u8,
        flags: posix.O,
        mode: posix.mode_t,
        ctx: Context,
    ) Allocator.Error!*Task {
        const task = try self.getTask();
        task.* = .{
            .userdata = ctx.ptr,
            .msg = ctx.msg,
            .callback = ctx.cb,
            .req = .{ .open = .{ .path = path, .flags = flags, .mode = mode } },
        };

        self.submission_q.push(task);
        return task;
    }

    /// Spawns a thread with a Ring instance. The thread will be idle and waiting to receive work
    /// via msgRing when this function returns. Call kill on the returned thread to signal it to
    /// shutdown.
    pub fn spawnThread(self: *Ring, entries: u16) !*Thread {
        const thread = try self.gpa.create(Thread);
        errdefer self.gpa.destroy(thread);

        var wg: std.Thread.WaitGroup = .{};
        wg.start();
        thread.thread = try std.Thread.spawn(.{}, Thread.run, .{ thread, self, &wg, entries });
        wg.wait();

        return thread;
    }
};

pub const Thread = struct {
    thread: std.Thread,
    ring: io.Ring = undefined,

    pub const Msg = enum {
        kill,
    };

    pub fn run(self: *Thread, parent: *io.Ring, wg: *std.Thread.WaitGroup, entries: u16) !void {
        self.ring = try parent.initChild(entries);
        wg.finish();

        defer self.ring.deinit();

        // Run forever, because we may not start with a task. Inter-thread messaging means we could
        // receive work at any time
        self.ring.run(.forever) catch |err| {
            switch (err) {
                error.ThreadKilled => return,
                else => return err,
            }
        };
    }

    /// Kill sends a message to the thread telling it to exit. Callers of this thread can safely
    /// join and deinit the Thread in the Context callback
    pub fn kill(self: *Thread, rt: *io.Ring, ctx: Context) Allocator.Error!*io.Task {
        const target_task = try rt.getTask();
        target_task.* = .{
            .userdata = self,
            .msg = @intFromEnum(Thread.Msg.kill),
            .callback = Thread.onCompletion,
            .result = .noop,
        };

        return rt.msgRing(&self.ring, target_task, ctx);
    }

    pub fn join(self: Thread) void {
        self.thread.join();
    }

    fn onCompletion(_: *io.Ring, task: Task) anyerror!void {
        switch (task.msgToEnum(Thread.Msg)) {
            .kill => return error.ThreadKilled,
        }
    }
};

pub const Op = enum {
    noop,
    deadline,
    timer,
    cancel,
    accept,
    msg_ring,
    recv,
    write,
    writev,
    close,
    poll,
    socket,
    connect,
    statx,
    readv,
    open,
    read,

    /// userbytes is meant to send slices of bytes between Ring instances or callbacks
    userbytes,
    /// userfd is meant to send file descriptors between Ring instances (using msgRing)
    userfd,
    /// usermsg is meant to send a u16 between runtime instances (using msgRing)
    usermsg,
    /// userptr is meant to send a pointer between runtime instances (using msgRing)
    userptr,
};

pub const Offset = enum(u64) {
    file = @bitCast(@as(i64, -1)),
    beginning = 0,
    _,

    pub fn bytes(n: anytype) Offset {
        return @enumFromInt(n);
    }
};

pub const Request = union(Op) {
    noop,
    deadline: Timespec,
    timer: Timespec,
    cancel: union(enum) {
        all,
        task: *Task,
    },
    accept: posix.fd_t,
    msg_ring: struct {
        target: *Ring,
        task: *Task,
    },
    recv: struct {
        fd: posix.fd_t,
        buffer: []u8,
    },
    write: struct {
        fd: posix.fd_t,
        buffer: []const u8,
        offset: Offset,
    },
    writev: struct {
        fd: posix.fd_t,
        vecs: []const posix.iovec_const,
        offset: Offset,
    },
    close: posix.fd_t,
    poll: struct {
        fd: posix.fd_t,
        mask: u32,
    },
    socket: struct {
        domain: u32,
        type: u32,
        protocol: u32,
    },
    connect: struct {
        fd: posix.socket_t,
        addr: *posix.sockaddr,
        addr_len: posix.socklen_t,
    },
    statx: struct {
        path: [:0]const u8,
        result: *Statx, // this will be filled in by the op
        symlink_follow: bool = true,
    },
    readv: struct {
        fd: posix.fd_t,
        vecs: []const posix.iovec,
        offset: Offset,
    },
    open: struct {
        path: [:0]const u8,
        flags: posix.O,
        mode: posix.mode_t,
    },
    read: struct {
        fd: posix.fd_t,
        buffer: []u8,
        offset: Offset,
    },

    userbytes,
    userfd,
    usermsg,
    userptr,
};

pub const Result = union(Op) {
    noop,
    deadline: ResultError!void,
    timer: ResultError!void,
    cancel: CancelError!void,
    accept: ResultError!posix.fd_t,
    msg_ring: ResultError!void,
    recv: RecvError!usize,
    write: ResultError!usize,
    writev: ResultError!usize,
    close: ResultError!void,
    poll: ResultError!void,
    socket: ResultError!posix.fd_t,
    connect: ResultError!void,
    statx: ResultError!*Statx,
    readv: ResultError!usize,
    open: OpenError!posix.fd_t,
    read: ResultError!usize,

    userbytes: anyerror![]const u8,
    userfd: anyerror!posix.fd_t,
    usermsg: u16,
    userptr: anyerror!?*anyopaque,
};

pub const ResultError = error{
    /// The request was invalid
    Invalid,
    /// The request was canceled
    Canceled,
    /// An unexpected error occured
    Unexpected,
};

pub const CancelError = ResultError || error{
    /// The entry to cancel couldn't be found
    EntryNotFound,
    /// The entry couldn't be canceled
    NotCanceled,
};

pub const RecvError = ResultError || error{
    /// The entry to cancel couldn't be found
    ConnectionResetByPeer,
};

pub const OpenError = ResultError || posix.OpenError;

test {
    _ = @import("ourio/Mock.zig");
    _ = @import("ourio/queue.zig");

    if (has_io_uring) _ = @import("ourio/Uring.zig");
    if (has_kqueue) _ = @import("ourio/Kqueue.zig");
}

/// Foo is only for testing
const Foo = struct {
    bar: usize = 0,

    fn callback(_: *io.Ring, task: io.Task) anyerror!void {
        const self = task.userdataCast(Foo);
        self.bar += 1;
    }
};

/// Follows the ABI of linux statx. Not all platforms will contain all information, or may contain
/// more information than requested
pub const Statx = extern struct {
    /// Mask of bits indicating filled fields
    mask: u32,

    /// Block size for filesystem I/O
    blksize: u32,

    /// Extra file attribute indicators
    attributes: u64,

    /// Number of hard links
    nlink: u32,

    /// User ID of owner
    uid: posix.uid_t,

    /// Group ID of owner
    gid: posix.gid_t,

    /// File type and mode
    mode: u16,
    __pad1: u16,

    /// Inode number
    ino: u64,

    /// Total size in bytes
    size: u64,

    /// Number of 512B blocks allocated
    blocks: u64,

    /// Mask to show what's supported in `attributes`.
    attributes_mask: u64,

    /// Last access file timestamp
    atime: Timestamp,

    /// Creation file timestamp
    btime: Timestamp,

    /// Last status change file timestamp
    ctime: Timestamp,

    /// Last modification file timestamp
    mtime: Timestamp,

    /// Major ID, if this file represents a device.
    rdev_major: u32,

    /// Minor ID, if this file represents a device.
    rdev_minor: u32,

    /// Major ID of the device containing the filesystem where this file resides.
    dev_major: u32,

    /// Minor ID of the device containing the filesystem where this file resides.
    dev_minor: u32,

    __pad2: [14]u64,

    pub const Timestamp = extern struct {
        sec: i64,
        nsec: u32,
        __pad: u32 = 0,
    };

    pub fn major(dev: u64) u32 {
        return switch (@import("builtin").target.os.tag) {
            .macos, .visionos, .tvos, .ios, .watchos => @intCast((dev >> 24) & 0xff),
            .freebsd, .openbsd, .netbsd, .dragonfly => @intCast((dev >> 8) & 0xff),
            else => @compileError("unsupported OS for major()"),
        };
    }

    pub fn minor(dev: u64) u32 {
        return switch (@import("builtin").target.os.tag) {
            .macos, .ios, .visionos, .tvos, .watchos => @intCast(dev & 0xffffff),
            .openbsd => @intCast(dev & 0xff),
            .freebsd => @intCast((dev & 0xff) | ((dev >> 12) & 0xfff00)),
            .dragonfly => @intCast((dev & 0xff) | ((dev >> 12) & 0xfff00)),
            .netbsd => @intCast((dev & 0xff) | ((dev >> 12) & 0xfff00)),
            else => @compileError("unsupported OS for minor()"),
        };
    }
};

test "runtime: noop" {
    var rt: io.Ring = try .init(std.testing.allocator, 16);
    defer rt.deinit();

    var foo: Foo = .{};

    const ctx: Context = .{ .ptr = &foo, .cb = Foo.callback };

    // noop is triggered synchronously with submit. If we wait, we'll be waiting forever
    _ = try rt.noop(ctx);
    try rt.run(.once);
    try std.testing.expectEqual(1, foo.bar);
    _ = try rt.noop(ctx);
    _ = try rt.noop(ctx);
    try rt.run(.once);
    try std.testing.expectEqual(3, foo.bar);
}

test "runtime: timer" {
    var rt: io.Ring = try .init(std.testing.allocator, 16);
    defer rt.deinit();

    var foo: Foo = .{};

    const ctx: Context = .{ .ptr = &foo, .cb = Foo.callback };

    const start = std.time.nanoTimestamp();
    const end = start + 100 * std.time.ns_per_ms;
    _ = try rt.timer(.{ .nsec = 100 * std.time.ns_per_ms }, ctx);
    try rt.run(.once);
    try std.testing.expect(std.time.nanoTimestamp() > end);
    try std.testing.expectEqual(1, foo.bar);
}

test "runtime: poll" {
    var rt: io.Ring = try .init(std.testing.allocator, 16);
    defer rt.deinit();

    var foo: Foo = .{};
    const pipe = try posix.pipe2(.{ .CLOEXEC = true });

    const ctx: Context = .{ .ptr = &foo, .cb = Foo.callback };

    _ = try rt.poll(pipe[0], posix.POLL.IN, ctx);
    try std.testing.expectEqual(1, rt.submission_q.len());

    _ = try posix.write(pipe[1], "io_uring is the best");
    try rt.run(.once);
    try std.testing.expectEqual(1, foo.bar);
}

test "runtime: deadline doesn't call user callback" {
    const gpa = std.testing.allocator;
    var rt = try io.Ring.init(gpa, 16);
    defer rt.deinit();

    var foo: Foo = .{};
    const ctx: Context = .{ .ptr = &foo, .cb = Foo.callback };
    const task = try rt.noop(ctx);
    try task.setDeadline(&rt, .{ .sec = 1 });

    try rt.run(.until_done);

    // Callback only called once
    try std.testing.expectEqual(1, foo.bar);
}

test "runtime: timeout" {
    const gpa = std.testing.allocator;
    var rt = try io.Ring.init(gpa, 16);
    defer rt.deinit();

    var foo: Foo = .{};
    const ctx: Context = .{ .ptr = &foo, .cb = Foo.callback };

    const delay = 1 * std.time.ns_per_ms;
    _ = try rt.timer(.{ .nsec = delay }, ctx);

    const start = std.time.nanoTimestamp();
    try rt.run(.until_done);
    try std.testing.expect(start + delay < std.time.nanoTimestamp());
    try std.testing.expectEqual(1, foo.bar);
}

test "runtime: cancel" {
    const gpa = std.testing.allocator;
    var rt = try io.Ring.init(gpa, 16);
    defer rt.deinit();

    var foo: Foo = .{};
    const ctx: Context = .{ .ptr = &foo, .cb = Foo.callback };

    const delay = 1 * std.time.ns_per_s;
    const task = try rt.timer(.{ .nsec = delay }, ctx);

    try task.cancel(&rt, .{});

    const start = std.time.nanoTimestamp();
    try rt.run(.until_done);
    // Expect that we didn't delay long enough
    try std.testing.expect(start + delay > std.time.nanoTimestamp());
    try std.testing.expectEqual(1, foo.bar);
}

test "runtime: cancel all" {
    const gpa = std.testing.allocator;
    var rt = try io.Ring.init(gpa, 16);
    defer rt.deinit();

    const Foo2 = struct {
        bar: usize = 0,

        fn callback(_: *io.Ring, task: io.Task) anyerror!void {
            const self = task.userdataCast(@This());
            const result = task.result.?;
            _ = result.timer catch |err| {
                switch (err) {
                    error.Canceled => self.bar += 1,
                    else => {},
                }
            };
        }
    };
    var foo: Foo2 = .{};
    const ctx: Context = .{ .ptr = &foo, .cb = Foo2.callback };

    const delay = 1 * std.time.ns_per_s;
    _ = try rt.timer(.{ .nsec = delay }, ctx);
    _ = try rt.timer(.{ .nsec = delay }, ctx);
    _ = try rt.timer(.{ .nsec = delay }, ctx);
    _ = try rt.timer(.{ .nsec = delay }, ctx);

    try rt.cancelAll();
    const start = std.time.nanoTimestamp();
    try rt.run(.until_done);
    // Expect that we didn't delay long enough
    try std.testing.expect(start + delay > std.time.nanoTimestamp());
    try std.testing.expectEqual(4, foo.bar);
}

test "runtime: msgRing" {
    const gpa = std.testing.allocator;
    var rt1 = try io.Ring.init(gpa, 16);
    defer rt1.deinit();

    var rt2 = try rt1.initChild(16);
    defer rt2.deinit();

    const Foo2 = struct {
        rt1: bool = false,
        rt2: bool = false,

        const Msg = enum { rt1, rt2 };

        fn callback(_: *io.Ring, task: io.Task) anyerror!void {
            const self = task.userdataCast(@This());
            const msg = task.msgToEnum(Msg);
            switch (msg) {
                .rt1 => self.rt1 = true,
                .rt2 => self.rt2 = true,
            }
        }
    };

    var foo: Foo2 = .{};

    // The task we will send from rt1 to rt2
    const target_task = try rt1.getTask();
    target_task.* = .{
        .userdata = &foo,
        .callback = Foo2.callback,
        .msg = @intFromEnum(Foo2.Msg.rt2),
        .result = .{ .usermsg = 0 },
    };

    _ = try rt1.msgRing(
        &rt2,
        target_task,
        .{ .cb = Foo2.callback, .msg = @intFromEnum(Foo2.Msg.rt1), .ptr = &foo },
    );

    try rt1.run(.until_done);
    try std.testing.expect(foo.rt1);
    try std.testing.expect(!foo.rt2);
    try rt2.run(.until_done);
    try std.testing.expect(foo.rt1);
    try std.testing.expect(foo.rt2);
}

test "runtime: spawnThread" {
    const gpa = std.testing.allocator;
    var rt = try io.Ring.init(gpa, 16);
    defer rt.deinit();

    const thread = try rt.spawnThread(4);

    const Foo2 = struct {
        kill: bool = false,
        did_work: bool = false,

        gpa: Allocator,
        thread: *Thread,

        const Msg = enum { kill, work };

        fn callback(_: *io.Ring, task: io.Task) anyerror!void {
            const self = task.userdataCast(@This());
            const msg = task.msgToEnum(Msg);
            switch (msg) {
                .kill => {
                    self.kill = true;
                    self.thread.join();
                    self.gpa.destroy(self.thread);
                },
                .work => self.did_work = true,
            }
        }
    };

    var foo: Foo2 = .{ .thread = thread, .gpa = gpa };

    // Send work to the thread
    const target_task = try rt.getTask();
    target_task.* = .{
        .userdata = &foo,
        .callback = Foo2.callback,
        .msg = @intFromEnum(Foo2.Msg.work),
        .result = .{ .usermsg = 0 },
    };

    _ = try rt.msgRing(&thread.ring, target_task, .{});

    try rt.run(.until_done);
    _ = try thread.kill(&rt, .{
        .ptr = &foo,
        .cb = Foo2.callback,
        .msg = @intFromEnum(Foo2.Msg.kill),
    });
    try rt.run(.until_done);

    try std.testing.expect(foo.did_work);
    try std.testing.expect(foo.kill);
}

test "runtime: stat" {
    const gpa = std.testing.allocator;
    var rt = try io.Ring.init(gpa, 16);
    defer rt.deinit();

    var foo: Foo = .{};
    const ctx: Context = .{ .ptr = &foo, .cb = Foo.callback };

    var statx: Statx = undefined;
    const task = try rt.stat("build.zig", &statx, ctx);

    try rt.run(.until_done);
    try std.testing.expect(task.result != null);
}
