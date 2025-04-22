const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const io_mod = b.addModule("ourio", .{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    const tls_dep = b.dependency("tls", .{ .target = target, .optimize = optimize });
    io_mod.addImport("tls", tls_dep.module("tls"));

    const lib_unit_tests = b.addTest(.{
        .root_module = io_mod,
    });

    const run_lib_unit_tests = b.addRunArtifact(lib_unit_tests);

    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_unit_tests.step);
}
