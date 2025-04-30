const std = @import("std");

pub const net = @import("stda/net.zig");
pub const tls = @import("stda/tls.zig");

const root = @import("root");
pub const options: Options = if (@hasDecl(root, "stda_options")) root.std_options else .{};

pub const Options = struct {
    /// nameservers to be used if system nameservers can't be located
    nameservers: [2]std.net.Address = .{
        std.net.Address.initIp4(.{ 1, 1, 1, 1 }, 53),
        std.net.Address.initIp4(.{ 1, 0, 0, 1 }, 53),
    },
};

test {
    _ = net;
    _ = tls;
}
