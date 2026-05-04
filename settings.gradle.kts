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
            substitute(module("farm.query.vgirpc:vgirpc"))
                .using(project(":vgirpc"))
        }
    }
}

include("vgi-core")
include("vgi-example-worker")
