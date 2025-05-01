const std = @import("std");
const stda = @import("../../stda.zig");
const ourio = @import("ourio");

const Allocator = std.mem.Allocator;
const Context = ourio.Context;
const Ring = ourio.Ring;
const Task = ourio.Task;
const assert = std.debug.assert;
const net = stda.net;
const posix = std.posix;

const default_dns = &stda.options.nameservers;

pub const Resolver = struct {
    gpa: Allocator,
    ctx: Context,
    config: Config = .{},

    const Msg = enum { open_resolv, read_resolv };

    /// initialize a Resolver instance. When the resolver is complete with initialization, a userptr
    /// result type will be delivered to ctx. The resolver will then be ready to resolve DNS queries
    pub fn init(
        self: *Resolver,
        gpa: Allocator,
        io: *Ring,
        ctx: Context,
    ) Allocator.Error!void {
        self.* = .{
            .gpa = gpa,
            .ctx = ctx,
        };

        _ = try io.open("/etc/resolv.conf", .{ .CLOEXEC = true }, 0, .{
            .cb = Resolver.onCompletion,
            .ptr = self,
            .msg = @intFromEnum(Msg.open_resolv),
        });
    }

    pub fn deinit(self: *Resolver) void {
        self.gpa.free(self.config.nameservers);
    }

    pub fn resolveQuery(self: *Resolver, io: *Ring, query: Question, ctx: ourio.Context) !void {
        assert(self.config.nameservers.len > 0);

        const conn = try self.gpa.create(Connection);
        conn.* = .{ .gpa = self.gpa, .ctx = ctx, .config = self.config };
        try conn.writeQuestion(query);

        try conn.tryNext(io);
    }

    pub fn onCompletion(io: *Ring, task: Task) anyerror!void {
        const self = task.userdataCast(Resolver);
        const msg = task.msgToEnum(Resolver.Msg);
        const result = task.result.?;

        switch (msg) {
            .open_resolv => {
                const fd = result.open catch {
                    self.config.nameservers = try self.gpa.dupe(std.net.Address, default_dns);
                    const t: Task = .{
                        .callback = self.ctx.cb,
                        .msg = self.ctx.msg,
                        .userdata = self.ctx.ptr,
                        .result = .{ .userptr = self },
                    };
                    try self.ctx.cb(io, t);
                    return;
                };

                const buffer = try self.gpa.alloc(u8, 4096);
                errdefer self.gpa.free(buffer);

                _ = try io.read(fd, buffer, .{
                    .cb = Resolver.onCompletion,
                    .ptr = self,
                    .msg = @intFromEnum(Resolver.Msg.read_resolv),
                });
            },

            .read_resolv => {
                const buffer = task.req.read.buffer;
                defer self.gpa.free(buffer);

                _ = try io.close(task.req.read.fd, .{});

                const n = result.read catch |err| {
                    const t: Task = .{
                        .callback = self.ctx.cb,
                        .msg = self.ctx.msg,
                        .userdata = self.ctx.ptr,
                        .result = .{ .userptr = err },
                    };
                    try self.ctx.cb(io, t);
                    return;
                };

                if (n >= buffer.len) {
                    @panic("TODO: more to read");
                }

                var line_iter = std.mem.splitScalar(u8, buffer[0..n], '\n');
                var addresses: std.ArrayListUnmanaged(std.net.Address) = .empty;
                defer addresses.deinit(self.gpa);

                while (line_iter.next()) |line| {
                    if (line.len == 0 or line[0] == ';' or line[0] == '#') continue;

                    var iter = std.mem.splitAny(u8, line, &std.ascii.whitespace);
                    const key = iter.first();

                    if (std.mem.eql(u8, key, "nameserver")) {
                        const addr = try std.net.Address.parseIp(iter.rest(), 53);
                        try addresses.append(self.gpa, addr);
                        continue;
                    }

                    if (std.mem.eql(u8, key, "options")) {
                        while (iter.next()) |opt| {
                            if (std.mem.startsWith(u8, opt, "timeout:")) {
                                const timeout = std.fmt.parseInt(u5, opt[8..], 10) catch 30;
                                self.config.timeout_s = @min(30, timeout);
                                continue;
                            }

                            if (std.mem.startsWith(u8, opt, "attempts:")) {
                                const attempts = std.fmt.parseInt(u3, opt[9..], 10) catch 5;
                                self.config.attempts = @max(@min(5, attempts), 1);
                                continue;
                            }

                            if (std.mem.eql(u8, opt, "edns0")) {
                                self.config.edns0 = true;
                                continue;
                            }
                        }
                    }
                }

                self.config.nameservers = try addresses.toOwnedSlice(self.gpa);

                const t: Task = .{
                    .callback = self.ctx.cb,
                    .msg = self.ctx.msg,
                    .userdata = self.ctx.ptr,
                    .result = .{ .userptr = self },
                };
                try self.ctx.cb(io, t);
            },
        }
    }
};

pub const Config = struct {
    nameservers: []const std.net.Address = &.{},

    /// timeout_s is silently capped to 30 according to man resolv.conf
    timeout_s: u5 = 30,

    /// attempts is capped at 5
    attempts: u3 = 5,

    edns0: bool = false,
};

pub const Header = packed struct(u96) {
    id: u16 = 0,

    flags1: packed struct(u8) {
        recursion_desired: bool = true,
        truncated: bool = false,
        authoritative_answer: bool = false,
        opcode: enum(u4) {
            query = 0,
            inverse_query = 1,
            server_status_request = 2,
        } = .query,
        is_response: bool = false,
    } = .{},

    flags2: packed struct(u8) {
        response_code: enum(u4) {
            success = 0,
            format_error = 1,
            server_failure = 2,
            name_error = 3,
            not_implemented = 4,
            refuse = 5,
        } = .success,
        z: u3 = 0,
        recursion_available: bool = false,
    } = .{},

    question_count: u16 = 0,

    answer_count: u16 = 0,

    authority_count: u16 = 0,

    additional_count: u16 = 0,

    pub fn asBytes(self: Header) [12]u8 {
        var bytes: [12]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&bytes);
        fbs.writer().writeInt(u16, self.id, .big) catch unreachable;

        fbs.writer().writeByte(@bitCast(self.flags1)) catch unreachable;
        fbs.writer().writeByte(@bitCast(self.flags2)) catch unreachable;

        fbs.writer().writeInt(u16, self.question_count, .big) catch unreachable;
        fbs.writer().writeInt(u16, self.answer_count, .big) catch unreachable;
        fbs.writer().writeInt(u16, self.authority_count, .big) catch unreachable;
        fbs.writer().writeInt(u16, self.additional_count, .big) catch unreachable;
        assert(fbs.pos == 12);
        return bytes;
    }
};

pub const Question = struct {
    host: []const u8,
    type: ResourceType = .A,
    class: enum(u16) {
        IN = 1,
        // CS = 2,
        // CH = 3,
        // HS = 4,
        // WILDCARD = 255,
    } = .IN,
};

pub const ResourceType = enum(u16) {
    A = 1,
    // NS = 2,
    // MD = 3,
    // MF = 4,
    // CNAME = 5,
    // SOA = 6,
    // MB = 7,
    // MG = 8,
    // MR = 9,
    // NULL = 10,
    // WKS = 11,
    // PTR = 12,
    // HINFO = 13,
    // MINFO = 14,
    // MX = 15,
    // TXT = 16,
    AAAA = 28,
    // SRV = 33,
    // OPT = 41,
};

pub const Answer = union(ResourceType) {
    A: [4]u8,
    AAAA: [16]u8,
};

pub const Response = struct {
    bytes: []const u8,

    pub fn header(self: Response) Header {
        assert(self.bytes.len >= 12);
        const readInt = std.mem.readInt;

        return .{
            .id = readInt(u16, self.bytes[0..2], .big),
            .flags1 = @bitCast(self.bytes[2]),
            .flags2 = @bitCast(self.bytes[3]),
            .question_count = readInt(u16, self.bytes[4..6], .big),
            .answer_count = readInt(u16, self.bytes[6..8], .big),
            .authority_count = readInt(u16, self.bytes[8..10], .big),
            .additional_count = readInt(u16, self.bytes[10..12], .big),
        };
    }

    pub const AnswerIterator = struct {
        bytes: []const u8,
        /// offset into bytes
        offset: usize = 0,

        count: usize,
        /// number of answers we have returned
        idx: usize = 0,

        pub fn next(self: *AnswerIterator) ?Answer {
            if (self.idx >= self.count or self.offset >= self.bytes.len) return null;
            defer self.idx += 1;

            // Read the name
            const b = self.bytes[self.offset];
            if (b & 0b1100_0000 == 0) {
                // Encoded name. Get past this
                self.offset = std.mem.indexOfScalar(u8, self.bytes[self.idx..], 0x00) orelse
                    return null;
            } else {
                // Name is pointer, we can advance 2 bytes
                self.offset += 2;
            }

            const typ: ResourceType = @enumFromInt(std.mem.readInt(
                u16,
                self.bytes[self.offset..][0..2],
                .big,
            ));
            self.offset += 2;
            const class = std.mem.readInt(u16, self.bytes[self.offset..][0..2], .big);
            assert(class == 1);
            self.offset += 2;
            const ttl = std.mem.readInt(u32, self.bytes[self.offset..][0..4], .big);
            _ = ttl;
            self.offset += 4;
            const rd_len = std.mem.readInt(u16, self.bytes[self.offset..][0..2], .big);
            self.offset += 2;
            defer self.offset += rd_len;

            switch (typ) {
                .A => {
                    assert(rd_len == 4);
                    return .{ .A = .{
                        self.bytes[self.offset],
                        self.bytes[self.offset + 1],
                        self.bytes[self.offset + 2],
                        self.bytes[self.offset + 3],
                    } };
                },

                .AAAA => {
                    assert(rd_len == 4);
                    return .{ .AAAA = .{
                        self.bytes[self.offset],
                        self.bytes[self.offset + 1],
                        self.bytes[self.offset + 2],
                        self.bytes[self.offset + 3],
                        self.bytes[self.offset + 4],
                        self.bytes[self.offset + 5],
                        self.bytes[self.offset + 6],
                        self.bytes[self.offset + 7],
                        self.bytes[self.offset + 8],
                        self.bytes[self.offset + 9],
                        self.bytes[self.offset + 10],
                        self.bytes[self.offset + 11],
                        self.bytes[self.offset + 12],
                        self.bytes[self.offset + 13],
                        self.bytes[self.offset + 14],
                        self.bytes[self.offset + 15],
                    } };
                },
            }
        }
    };

    pub fn answerIterator(self: Response) !AnswerIterator {
        const h = self.header();

        var offset: usize = 12;

        var q: u16 = 0;
        while (q < h.question_count) {
            offset = std.mem.indexOfScalarPos(u8, self.bytes, offset, 0x00) orelse
                return error.InvalidResponse;
            offset += 4; // 2 bytes for type, 2 bytes for class
            q += 1;
        }

        return .{
            .bytes = self.bytes[offset..],
            .count = h.answer_count,
        };
    }
};

pub const Connection = struct {
    gpa: Allocator,
    ctx: Context,
    config: Config,

    nameserver: u8 = 0,
    attempt: u5 = 0,

    read_buffer: [2048]u8 = undefined,
    write_buffer: std.ArrayListUnmanaged(u8) = .empty,
    deadline: i64 = 0,

    const Msg = enum { connect, recv };

    pub fn tryNext(self: *Connection, io: *Ring) !void {
        self.deadline = std.time.timestamp() + self.config.timeout_s;

        if (self.attempt < self.config.attempts) {
            const addr = self.config.nameservers[self.nameserver];
            self.attempt += 1;

            _ = try net.udpConnectToAddr(io, addr, .{
                .cb = Connection.onCompletion,
                .msg = @intFromEnum(Connection.Msg.connect),
                .ptr = self,
            });

            return;
        }

        self.attempt = 0;

        if (self.nameserver < self.config.nameservers.len) {
            const addr = self.config.nameservers[self.nameserver];
            self.nameserver += 1;

            _ = try net.udpConnectToAddr(io, addr, .{
                .cb = Connection.onCompletion,
                .msg = @intFromEnum(Connection.Msg.connect),
                .ptr = self,
            });
            return;
        }

        defer self.gpa.destroy(self);
        try self.sendResult(io, .{ .userbytes = error.Timeout });
    }

    pub fn onCompletion(io: *Ring, task: Task) anyerror!void {
        const self = task.userdataCast(Connection);
        const msg = task.msgToEnum(Connection.Msg);
        const result = task.result.?;

        switch (msg) {
            .connect => {
                const fd = result.userfd catch return self.tryNext(io);

                const recv_task = try io.recv(fd, &self.read_buffer, .{
                    .cb = Connection.onCompletion,
                    .ptr = self,
                    .msg = @intFromEnum(Connection.Msg.recv),
                });
                try recv_task.setDeadline(io, .{ .sec = self.deadline });

                const write_task = try io.write(fd, self.write_buffer.items, .{});
                try write_task.setDeadline(io, .{ .sec = self.deadline });
            },

            .recv => {
                const n = result.recv catch {
                    _ = try io.close(task.req.recv.fd, .{});
                    return self.tryNext(io);
                };

                if (n == 0) {
                    _ = try io.close(task.req.recv.fd, .{});
                    return self.tryNext(io);
                }

                try self.sendResult(io, .{ .userbytes = self.read_buffer[0..n] });
                _ = try io.close(task.req.recv.fd, .{});
                self.gpa.destroy(self);
            },
        }
    }

    fn sendResult(self: *Connection, io: *Ring, result: ourio.Result) !void {
        defer self.write_buffer.deinit(self.gpa);
        const task: ourio.Task = .{
            .callback = self.ctx.cb,
            .userdata = self.ctx.ptr,
            .msg = self.ctx.msg,
            .result = result,
        };
        try self.ctx.cb(io, task);
    }

    fn writeQuestion(self: *Connection, query: Question) !void {
        const header: Header = .{ .question_count = 1 };
        var writer = self.write_buffer.writer(self.gpa);
        try writer.writeAll(&header.asBytes());

        var iter = std.mem.splitScalar(u8, query.host, '.');
        while (iter.next()) |val| {
            const len: u8 = @intCast(val.len);
            try writer.writeByte(len);
            try writer.writeAll(val);
        }
        try writer.writeByte(0x00);
        try writer.writeInt(u16, @intFromEnum(query.type), .big);
        try writer.writeInt(u16, @intFromEnum(query.class), .big);
    }
};

test "Resolver" {
    const Anon = struct {
        fn onOpen(_: *Task) ourio.Result {
            return .{ .open = 1 };
        }

        fn onRead(task: *Task) ourio.Result {
            const @"resolv.conf" =
                \\nameserver 1.1.1.1
                \\nameserver 1.0.0.1
                \\options timeout:10 attempts:3
            ;
            @memcpy(task.req.read.buffer[0..@"resolv.conf".len], @"resolv.conf");
            return .{ .read = @"resolv.conf".len };
        }

        fn onClose(_: *Task) ourio.Result {
            return .{ .close = {} };
        }

        fn onSocket(_: *Task) ourio.Result {
            return .{ .socket = 1 };
        }

        fn onConnect(_: *Task) ourio.Result {
            return .{ .connect = {} };
        }

        fn onRecv(_: *Task) ourio.Result {
            return .{ .recv = 1 };
        }

        fn onWrite(task: *Task) ourio.Result {
            return .{ .write = task.req.write.buffer.len };
        }
    };

    var io: ourio.Ring = try .initMock(std.testing.allocator, 16);
    defer io.deinit();

    io.backend.mock = .{
        .open_cb = Anon.onOpen,
        .read_cb = Anon.onRead,
        .close_cb = Anon.onClose,
        .socket_cb = Anon.onSocket,
        .connect_cb = Anon.onConnect,
        .recv_cb = Anon.onRecv,
        .write_cb = Anon.onWrite,
    };

    var resolver: Resolver = undefined;
    try resolver.init(std.testing.allocator, &io, .{});
    defer resolver.deinit();

    try std.testing.expectEqual(0, resolver.config.nameservers.len);
    try std.testing.expectEqual(5, resolver.config.attempts);
    try std.testing.expectEqual(30, resolver.config.timeout_s);

    try io.run(.until_done);

    try resolver.resolveQuery(&io, .{ .host = "timculverhouse.com" }, .{});
    try io.run(.until_done);
    try std.testing.expectEqual(2, resolver.config.nameservers.len);
    try std.testing.expectEqual(3, resolver.config.attempts);
    try std.testing.expectEqual(10, resolver.config.timeout_s);
}

test "Header roundtrip" {
    const header: Header = .{ .question_count = 1 };
    const bytes = header.asBytes();
    const response: Response = .{ .bytes = &bytes };
    const resp_header = response.header();
    try std.testing.expectEqual(header, resp_header);
}
