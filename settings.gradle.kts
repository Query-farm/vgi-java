// Copyright 2026 Query Farm LLC - https://query.farm
plugins {
    // Auto-provision Java 21 toolchain when not installed locally.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "vgi-java"

// Composite-include the sibling vgi-rpc-java repo so :vgirpc is built from
// source; falls back gracefully if the directory isn't present (CI may pin
// to a published artifact instead).
// VGI_RPC_JAVA_DIR overrides the path (CI checks out the sibling repo inside
// the workspace and points here so it can build vgirpc from source — actions/
// checkout can't write the default ../Development sibling path).
val vgiRpcJavaDir = System.getenv("VGI_RPC_JAVA_DIR")?.let { file(it) }
    ?: file("../Development/vgi-rpc-java")
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

include("vgi")
include("vgi-example-worker")
