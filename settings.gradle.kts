plugins {
    // Auto-provision Java 21 toolchain when not installed locally.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "vgi-java"

// Composite-include the sibling vgi-rpc-java repo so :vgirpc is built from
// source; falls back gracefully if the directory isn't present (CI may pin
// to a published artifact instead).
val vgiRpcJavaDir = file("../Development/vgi-rpc-java")
if (vgiRpcJavaDir.isDirectory) {
    includeBuild(vgiRpcJavaDir) {
        dependencySubstitution {
            // Coordinates match what the modules actually publish
            // (group=farm.query, see ../Development/vgi-rpc-java/build.gradle.kts).
            substitute(module("farm.query:vgirpc")).using(project(":vgirpc"))
            substitute(module("farm.query:vgirpc-oauth")).using(project(":vgirpc-oauth"))
            // Back-compat with callers that still use the legacy
            // farm.query.vgirpc:* coordinate naming.
            substitute(module("farm.query.vgirpc:vgirpc")).using(project(":vgirpc"))
            substitute(module("farm.query.vgirpc:vgirpc-oauth")).using(project(":vgirpc-oauth"))
        }
    }
}

include("vgi-core")
include("vgi-example-worker")
