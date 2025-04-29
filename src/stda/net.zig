const std = @import("std");
const io = @import("ourio");

const posix = std.posix;
const Allocator = std.mem.Allocator;
const Uri = std.Uri;

const assert = std.debug.assert;

pub fn tcpConnectToHost(
    rt: *io.Ring,
    host: []const u8,
    port: u16,
    ctx: io.Context,
) !*ConnectTask {
    // TODO: getAddressList could be rewritten to be async. It accesses the filesystem and could
    // make a DNS request
    const list = try std.net.getAddressList(rt.gpa, host, port);
    defer list.deinit();

    const addr = for (list.addrs) |addr| {
        break addr;
    } else return error.AddressNotFound;

    return tcpConnectToAddr(rt, addr, ctx);
}

pub fn tcpConnectToAddr(
    rt: *io.Ring,
    addr: std.net.Address,
    ctx: io.Context,
) Allocator.Error!*ConnectTask {
    const conn = try rt.gpa.create(ConnectTask);
    errdefer rt.gpa.destroy(conn);

    conn.* = .{
        .ctx = ctx,

        .addr = addr,
        .fd = null,
        .task = undefined,
    };

    conn.task = try rt.socket(
        conn.addr.any.family,
        posix.SOCK.STREAM | posix.SOCK.CLOEXEC,
        posix.IPPROTO.TCP,
        .{ .ptr = conn, .msg = @intFromEnum(ConnectTask.Msg.socket), .cb = ConnectTask.handleMsg },
    );

    return conn;
}

pub const ConnectTask = struct {
    ctx: io.Context,

    addr: std.net.Address,
    fd: ?posix.fd_t,

    /// Task is the current task we are operating on. We store this to provide cancelation
    task: *io.Task,

    pub const Msg = enum {
        socket,
        connect,
    };

    /// Cancels the current task. Not guaranteed to actually cancel. User's callback will get an
    /// error.Canceled if cancelation was successful, otherwise the operation will complete as
    /// normal and this is essentially a no-op
    pub fn cancel(self: *ConnectTask, rt: *io.Ring) void {
        _ = self.task.cancel(rt, null, 0, io.noopCallback) catch {};
    }

    pub fn handleMsg(rt: *io.Ring, task: io.Task) anyerror!void {
        const self = task.userdataCast(ConnectTask);
        const result = task.result.?;
        switch (task.msgToEnum(Msg)) {
            .socket => {
                assert(result == .socket);
                self.fd = result.socket catch |err| {
                    defer rt.gpa.destroy(self);
                    try self.ctx.cb(rt, .{
                        .userdata = self.ctx.ptr,
                        .msg = self.ctx.msg,
                        .result = .{ .userfd = err },
                        .callback = self.ctx.cb,
                        .req = .userfd,
                    });
                    return;
                };

                self.task = try rt.connect(
                    self.fd.?,
                    &self.addr.any,
                    self.addr.getOsSockLen(),
                    .{ .ptr = self, .msg = @intFromEnum(Msg.connect), .cb = ConnectTask.handleMsg },
                );
            },

            .connect => {
                assert(result == .connect);
                defer rt.gpa.destroy(self);

                _ = result.connect catch |err| {
                    try self.ctx.cb(rt, .{
                        .userdata = self.ctx.ptr,
                        .msg = self.ctx.msg,
                        .result = .{ .userfd = err },
                        .callback = self.ctx.cb,
                        .req = .userfd,
                    });
                    _ = try rt.close(self.fd.?, .{});
                    return;
                };

                try self.ctx.cb(rt, .{
                    .userdata = self.ctx.ptr,
                    .msg = self.ctx.msg,
                    .result = .{ .userfd = self.fd.? },
                    .callback = self.ctx.cb,
                    .req = .userfd,
                });
            },
        }
    }
};

test "tcp connect" {
    var rt: io.Ring = try .init(std.testing.allocator, 16);
    defer rt.deinit();

    const addr: std.net.Address = try .parseIp4("127.0.0.1", 80);

    {
        // Happy path
        const conn = try tcpConnectToAddr(&rt, addr, .{});
        errdefer std.testing.allocator.destroy(conn);

        const task1 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task1);
        try std.testing.expect(task1.req == .socket);
        try std.testing.expect(rt.submission_q.pop() == null);

        const fd: posix.fd_t = 7;
        try ConnectTask.handleMsg(&rt, .{
            .userdata = conn,
            .msg = @intFromEnum(ConnectTask.Msg.socket),
            .result = .{ .socket = fd },
            .req = .userfd,
        });

        const task2 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task2);
        try std.testing.expect(task2.req == .connect);
        try std.testing.expect(rt.submission_q.pop() == null);

        try ConnectTask.handleMsg(&rt, .{
            .userdata = conn,
            .msg = @intFromEnum(ConnectTask.Msg.connect),
            .result = .{ .connect = {} },
            .req = .userfd,
        });
        try std.testing.expect(rt.submission_q.pop() == null);
    }

    {
        // socket error
        const conn = try tcpConnectToAddr(&rt, addr, .{});
        errdefer std.testing.allocator.destroy(conn);

        const task1 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task1);

        try ConnectTask.handleMsg(&rt, .{
            .userdata = conn,
            .msg = @intFromEnum(ConnectTask.Msg.socket),
            .result = .{ .socket = error.Canceled },
            .req = .userfd,
        });
        try std.testing.expect(rt.submission_q.pop() == null);
    }

    {
        // connect error
        const conn = try tcpConnectToAddr(&rt, addr, .{});
        errdefer std.testing.allocator.destroy(conn);

        const task1 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task1);
        try std.testing.expect(task1.req == .socket);
        try std.testing.expect(rt.submission_q.pop() == null);

        const fd: posix.fd_t = 7;
        try ConnectTask.handleMsg(&rt, .{
            .userdata = conn,
            .msg = @intFromEnum(ConnectTask.Msg.socket),
            .result = .{ .socket = fd },
            .req = .userfd,
        });

        const task2 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task2);
        try std.testing.expect(task2.req == .connect);
        try std.testing.expect(rt.submission_q.pop() == null);

        try ConnectTask.handleMsg(&rt, .{
            .userdata = conn,
            .msg = @intFromEnum(ConnectTask.Msg.connect),
            .result = .{ .connect = error.Canceled },
            .req = .noop,
        });
        const task3 = rt.submission_q.pop().?;
        defer std.testing.allocator.destroy(task3);
        try std.testing.expect(task3.req == .close);
        try std.testing.expect(rt.submission_q.pop() == null);
    }
}
