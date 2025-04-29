const std = @import("std");

const Module = std.Build.Module;
const Target = std.Build.ResolvedTarget;
const OptimizeMode = std.builtin.OptimizeMode;

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const ourio_mod = b.addModule("ourio", .{
        .root_source_file = b.path("src/ourio.zig"),
        .target = target,
        .optimize = optimize,
    });

    const stda_mod = setupStdaMod(b, target, optimize, ourio_mod);

    const ourio_tests = b.addTest(.{ .root_module = ourio_mod });
    const stda_tests = b.addTest(.{ .root_module = stda_mod });

    const run_ourio_tests = b.addRunArtifact(ourio_tests);
    const run_stda_tests = b.addRunArtifact(stda_tests);
    run_ourio_tests.skip_foreign_checks = true;
    b.installArtifact(ourio_tests);

    const test_step = b.step("test", "Run all unit tests");
    test_step.dependOn(&run_ourio_tests.step);
    test_step.dependOn(&run_stda_tests.step);

    const test_ourio_step = b.step("test-ourio", "Run ourio unit tests");
    test_ourio_step.dependOn(&run_ourio_tests.step);

    const install_step = b.getInstallStep();
    install_step.dependOn(test_step);
}

pub fn setupStdaMod(b: *std.Build, target: Target, optimize: OptimizeMode, ourio: *Module) *Module {
    const stda_mod = b.addModule("stda", .{
        .root_source_file = b.path("src/stda.zig"),
        .target = target,
        .optimize = optimize,
    });
    const tls_dep = b.dependency("tls", .{ .target = target, .optimize = optimize });
    stda_mod.addImport("tls", tls_dep.module("tls"));
    stda_mod.addImport("ourio", ourio);

    return stda_mod;
}
