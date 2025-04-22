const std = @import("std");
const tls = @import("tls");
const io = @import("main.zig");

const Allocator = std.mem.Allocator;
const CertBundle = tls.config.cert.Bundle;
const assert = std.debug.assert;
const mem = std.mem;
const posix = std.posix;

pub const Client = struct {
    gpa: Allocator,
    fd: posix.fd_t,
    tls: tls.nonblock.Connection,
    recv_task: ?*io.Task = null,

    read_buf: [tls.max_ciphertext_record_len]u8 = undefined,
    read_end: usize = 0,

    cleartext_buf: std.ArrayListUnmanaged(u8) = .empty,
    ciphertext_buf: std.ArrayListUnmanaged(u8) = .empty,
    written: usize = 0,

    userdata: ?*anyopaque = null,
    callback: *const fn (*io.Runtime, io.Task) anyerror!void = io.noopCallback,
    close_msg: u16 = 0,
    write_msg: u16 = 0,
    recv_msg: u16 = 0,

    pub const HandshakeTask = struct {
        userdata: ?*anyopaque,
        callback: io.Callback,
        msg: u16,

        fd: posix.fd_t,
        buffer: [tls.max_ciphertext_record_len]u8 = undefined,
        read_end: usize = 0,
        handshake: tls.nonblock.Client,
        task: *io.Task,

        pub fn handleMsg(rt: *io.Runtime, task: io.Task) anyerror!void {
            const self = task.userdataCast(HandshakeTask);
            const result = task.result.?;

            switch (result) {
                .write => {
                    _ = result.write catch |err| {
                        defer rt.gpa.destroy(self);
                        // send the error to the callback
                        try self.callback(rt, .{
                            .userdata = self.userdata,
                            .msg = self.msg,
                            .result = .{ .userptr = err },
                            .callback = self.callback,
                            .req = .userptr,
                        });
                        return;
                    };

                    if (self.handshake.done()) {
                        defer rt.gpa.destroy(self);
                        // Handshake is done. Create a client and deliver it to the callback
                        const client = try self.initClient(rt.gpa);
                        try self.callback(rt, .{
                            .userdata = self.userdata,
                            .msg = self.msg,
                            .result = .{ .userptr = client },
                            .callback = self.callback,
                            .req = .userptr,
                        });
                        return;
                    }

                    // Arm a recv task
                    self.task = try rt.recv(self.fd, &self.buffer, .{
                        .ptr = self,
                        .cb = handleMsg,
                    });
                },

                .recv => {
                    const n = result.recv catch |err| {
                        defer rt.gpa.destroy(self);
                        // send the error to the callback
                        try self.callback(rt, .{
                            .userdata = self.userdata,
                            .msg = self.msg,
                            .result = .{ .userptr = err },
                            .callback = self.callback,
                            .req = .userptr,
                        });
                        return;
                    };

                    self.read_end += n;
                    const slice = self.buffer[0..self.read_end];
                    var scratch: [tls.max_ciphertext_record_len]u8 = undefined;
                    const r = try self.handshake.run(slice, &scratch);

                    if (r.unused_recv.len > 0) {
                        // Arm a recv task
                        self.task = try rt.recv(self.fd, self.buffer[self.read_end..], .{
                            .ptr = self,
                            .cb = handleMsg,
                        });
                        return;
                    }

                    if (r.send.len > 0) {
                        // Queue another send
                        @memcpy(self.buffer[0..r.send.len], r.send);
                        self.task = try rt.write(
                            self.fd,
                            self.buffer[0..r.send.len],
                            .{ .ptr = self, .cb = HandshakeTask.handleMsg },
                        );
                        return;
                    }

                    if (self.handshake.done()) {
                        defer rt.gpa.destroy(self);
                        // Handshake is done. Create a client and deliver it to the callback
                        const client = try self.initClient(rt.gpa);
                        try self.callback(rt, .{
                            .userdata = self.userdata,
                            .msg = self.msg,
                            .result = .{ .userptr = client },
                            .callback = self.callback,
                            .req = .userptr,
                        });
                        return;
                    }
                },

                else => unreachable,
            }
        }

        fn initClient(self: *HandshakeTask, gpa: Allocator) !*Client {
            const client = try gpa.create(Client);
            client.* = .{
                .gpa = gpa,
                .fd = self.fd,
                .tls = .{ .cipher = self.handshake.inner.cipher },
            };
            return client;
        }

        /// Tries to cancel the handshake. Callback will receive an error.Canceled if cancelation
        /// was successful, otherwise handhsake will proceed
        pub fn cancel(self: *HandshakeTask, rt: *io.Runtime) void {
            self.task.cancel(rt, null, 0, io.noopCallback) catch {};
        }
    };

    const Msg = enum {
        write,
        recv,
        close_notify,
    };

    /// Initializes a handshake, which will ultimately deliver a Client to the callback via a
    /// userptr result
    pub fn init(
        rt: *io.Runtime,
        fd: posix.fd_t,
        opts: tls.config.Client,
        ctx: io.Context,
    ) !*HandshakeTask {
        const hs = try rt.gpa.create(HandshakeTask);
        hs.* = .{
            .userdata = ctx.ptr,
            .callback = ctx.cb,
            .msg = ctx.msg,

            .fd = fd,
            .handshake = .init(opts),
            .task = undefined,
        };

        const result = try hs.handshake.run("", &hs.buffer);
        const hs_ctx: io.Context = .{ .ptr = hs, .cb = HandshakeTask.handleMsg };
        hs.task = try rt.write(hs.fd, result.send, hs_ctx);
        return hs;
    }

    pub fn deinit(self: *Client, gpa: Allocator) void {
        self.ciphertext_buf.deinit(gpa);
        self.cleartext_buf.deinit(gpa);
    }

    pub fn close(self: *Client, gpa: Allocator, rt: *io.Runtime) !void {
        // close notify is 2 bytes long
        const len = self.tls.encryptedLength(2);
        try self.ciphertext_buf.ensureUnusedCapacity(gpa, len);
        const buf = self.ciphertext_buf.unusedCapacitySlice();
        const msg = try self.tls.close(buf);

        self.ciphertext_buf.items.len += msg.len;
        _ = try rt.write(self.fd, self.ciphertext_buf.items[self.written..], .{
            .ptr = self,
            .cb = Client.onCompletion,
            .msg = @intFromEnum(Client.Msg.close_notify),
        });

        if (self.recv_task) |task| {
            try task.cancel(rt, .{});
            self.recv_task = null;
        }
    }

    fn onCompletion(rt: *io.Runtime, task: io.Task) anyerror!void {
        const self = task.userdataCast(Client);
        const result = task.result.?;

        switch (task.msgToEnum(Client.Msg)) {
            .recv => {
                assert(result == .recv);
                self.recv_task = null;
                const n = result.recv catch |err| {
                    return self.callback(rt, .{
                        .userdata = self.userdata,
                        .msg = self.recv_msg,
                        .callback = self.callback,
                        .req = .{ .recv = .{ .fd = self.fd, .buffer = &self.read_buf } },
                        .result = .{ .recv = err },
                    });
                };
                self.read_end += n;
                const end = self.read_end;
                const r = try self.tls.decrypt(self.read_buf[0..end], self.read_buf[0..end]);

                if (r.cleartext.len > 0) {
                    try self.callback(rt, .{
                        .userdata = self.userdata,
                        .msg = self.recv_msg,
                        .callback = self.callback,
                        .req = .{ .recv = .{ .fd = self.fd, .buffer = &self.read_buf } },
                        .result = .{ .recv = r.cleartext.len },
                    });
                }
                mem.copyForwards(u8, &self.read_buf, r.unused_ciphertext);
                self.read_end = r.unused_ciphertext.len;

                if (r.closed) {
                    _ = try rt.close(self.fd, self.closeContext());
                    return;
                }

                self.recv_task = try rt.recv(
                    self.fd,
                    self.read_buf[self.read_end..],
                    self.recvContext(),
                );
            },

            .write => {
                assert(result == .write);
                const n = result.write catch {
                    return self.callback(rt, .{
                        .userdata = self.userdata,
                        .msg = self.write_msg,
                        .callback = self.callback,
                        .req = .{ .write = .{ .fd = self.fd, .buffer = self.ciphertext_buf.items } },
                        .result = .{ .write = error.Unexpected },
                    });
                };
                self.written += n;

                if (self.written < self.ciphertext_buf.items.len) {
                    _ = try rt.write(
                        self.fd,
                        self.ciphertext_buf.items[self.written..],
                        self.writeContext(),
                    );
                } else {
                    defer {
                        self.written = 0;
                        self.ciphertext_buf.clearRetainingCapacity();
                    }
                    return self.callback(rt, .{
                        .userdata = self.userdata,
                        .msg = self.write_msg,
                        .callback = self.callback,
                        .req = .{ .write = .{ .fd = self.fd, .buffer = self.ciphertext_buf.items } },
                        .result = .{ .write = self.written },
                    });
                }
            },

            .close_notify => {
                assert(result == .write);
                const n = result.write catch {
                    return self.callback(rt, .{
                        .userdata = self.userdata,
                        .msg = self.close_msg,
                        .callback = self.callback,
                        .req = .{ .close = self.fd },
                        .result = .{ .close = error.Unexpected },
                    });
                };

                self.written += n;

                if (self.written < self.ciphertext_buf.items.len) {
                    _ = try rt.write(self.fd, self.ciphertext_buf.items[self.written..], .{
                        .ptr = self,
                        .cb = Client.onCompletion,
                        .msg = @intFromEnum(Client.Msg.close_notify),
                    });
                } else {
                    self.written = 0;
                    self.ciphertext_buf.clearRetainingCapacity();
                    _ = try rt.close(self.fd, self.closeContext());
                }
            },
        }
    }

    pub fn recv(self: *Client, rt: *io.Runtime) !void {
        if (self.recv_task != null) return;
        self.recv_task = try rt.recv(
            self.fd,
            self.read_buf[self.read_end..],
            self.recvContext(),
        );
    }

    pub fn write(self: *Client, gpa: Allocator, bytes: []const u8) Allocator.Error!void {
        try self.cleartext_buf.appendSlice(gpa, bytes);
    }

    pub fn flush(self: *Client, gpa: Allocator, rt: *io.Runtime) !void {
        const len = self.tls.encryptedLength(self.cleartext_buf.items.len);
        try self.ciphertext_buf.ensureUnusedCapacity(gpa, len);
        const slice = self.ciphertext_buf.unusedCapacitySlice();
        const result = try self.tls.encrypt(self.cleartext_buf.items, slice);
        self.ciphertext_buf.appendSliceAssumeCapacity(result.ciphertext);
        self.cleartext_buf.replaceRangeAssumeCapacity(0, result.cleartext_pos, "");

        _ = try rt.write(
            self.fd,
            self.ciphertext_buf.items.len,
            self,
            @intFromEnum(Client.Msg.write),
            Client.onCompletion,
        );
    }

    fn closeContext(self: Client) io.Context {
        return .{ .ptr = self.userdata, .cb = self.callback, .msg = self.close_msg };
    }

    fn recvContext(self: *Client) io.Context {
        return .{
            .ptr = self,
            .cb = Client.onCompletion,
            .msg = @intFromEnum(Client.Msg.recv),
        };
    }

    fn writeContext(self: *Client) io.Context {
        return .{
            .ptr = self,
            .cb = Client.onCompletion,
            .msg = @intFromEnum(Client.Msg.write),
        };
    }
};

test "tls: Client" {
    const net = @import("net.zig");
    const gpa = std.testing.allocator;

    var rt = try io.Runtime.init(gpa, 16);
    defer rt.deinit();

    const Foo = struct {
        const Self = @This();
        gpa: Allocator,
        fd: ?posix.fd_t = null,
        tls: ?*Client = null,

        const Msg = enum {
            connect,
            handshake,
            close,
            write,
            recv,
        };

        fn callback(_: *io.Runtime, task: io.Task) anyerror!void {
            const self = task.userdataCast(Self);
            const result = task.result.?;
            errdefer {
                if (self.tls) |client| {
                    client.deinit(self.gpa);
                    self.gpa.destroy(client);
                    self.tls = null;
                }
            }

            switch (task.msgToEnum(Msg)) {
                .connect => {
                    self.fd = try result.userfd;
                },
                .handshake => {
                    const ptr = try result.userptr;
                    self.tls = @ptrCast(@alignCast(ptr));
                    self.tls.?.userdata = self;
                    self.tls.?.close_msg = @intFromEnum(@This().Msg.close);
                    self.tls.?.write_msg = @intFromEnum(@This().Msg.write);
                    self.tls.?.recv_msg = @intFromEnum(@This().Msg.recv);
                    self.tls.?.callback = @This().callback;
                },
                .close => {
                    self.tls.?.deinit(self.gpa);
                    self.gpa.destroy(self.tls.?);
                    self.tls = null;
                    self.fd = null;
                },

                else => {},
            }
        }
    };

    var foo: Foo = .{ .gpa = gpa };
    defer {
        if (foo.tls) |client| {
            client.deinit(gpa);
            gpa.destroy(client);
        }
        if (foo.fd) |fd| posix.close(fd);
    }

    _ = try net.tcpConnectToHost(
        &rt,
        "google.com",
        443,
        .{ .ptr = &foo, .cb = Foo.callback, .msg = @intFromEnum(Foo.Msg.connect) },
    );

    try rt.run(.until_done);

    try std.testing.expect(foo.fd != null);

    var bundle: CertBundle = .{};
    try bundle.rescan(gpa);
    defer bundle.deinit(gpa);

    _ = try Client.init(
        &rt,
        foo.fd.?,
        .{ .root_ca = bundle, .host = "google.com" },
        .{ .ptr = &foo, .cb = Foo.callback, .msg = @intFromEnum(Foo.Msg.handshake) },
    );
    try rt.run(.until_done);
    try std.testing.expect(foo.tls != null);

    try foo.tls.?.recv(&rt);
    try foo.tls.?.close(gpa, &rt);
    try rt.run(.until_done);
    try std.testing.expect(foo.tls == null);
    try std.testing.expect(foo.fd == null);
}
