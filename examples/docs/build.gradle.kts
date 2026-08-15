// Copyright 2026 Query Farm LLC - https://query.farm
//
// The workers embedded verbatim in the vgi-java documentation — one per
// function kind, plus a catalog and a combined worker the tutorial attaches to.
//
// Distinct from :vgi-example-worker, which holds the fixtures the C++
// integration suite drives and is not meant to be read as example code.
//
// NOTE ON THE DEPENDENCY. These build against `project(":vgi")` so they track
// the SDK at HEAD and a breaking change shows up here rather than in a reader's
// editor. A reader does the same thing with one line instead:
//
//     implementation("farm.query:vgi:0.26.1")
//
// which is what the documentation shows. The toolchain, `-parameters` and the
// JVM opens all come from the root build, so nothing else is repeated here.
plugins {
    application
}

dependencies {
    implementation(project(":vgi"))
    // A logging backend. VGI uses SLF4J; pick any binding you like.
    runtimeOnly("org.slf4j:slf4j-simple:2.0.17")
}

application {
    // The launch script this produces is what ATTACH points at, so it is named
    // for what it is rather than for the Gradle project directory (":examples:docs"
    // would otherwise give `bin/docs`).
    applicationName = "vgi-java-examples"
    // The combined worker registers all five example functions plus the
    // catalog. It is what the tutorial and verify.sh attach to.
    mainClass.set("farm.query.vgi.examples.AllInOneWorker")
    applicationDefaultJvmArgs = listOf(
        // Arrow's off-heap memory module needs access to java.nio internals.
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        // The shared-memory transport makes FFM (mmap/shm_open) downcalls.
        "--enable-native-access=ALL-UNNAMED",
    )
}

// Each single-kind worker is independently runnable:
//   ./gradlew :examples:docs:runScalar --args="--unix /tmp/s.sock --idle-timeout 30"
val workerJvmArgs = listOf(
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--enable-native-access=ALL-UNNAMED",
)

fun registerWorker(task: String, mainCls: String) {
    tasks.register<JavaExec>(task) {
        group = "vgi-examples"
        description = "Run the $task worker (pass --args=\"--unix <path> --idle-timeout <s>\")."
        classpath = sourceSets["main"].runtimeClasspath
        mainClass.set(mainCls)
        jvmArgs(workerJvmArgs)
    }
}

registerWorker("runScalar", "farm.query.vgi.examples.ScalarExample")
registerWorker("runTable", "farm.query.vgi.examples.TableExample")
registerWorker("runTableInOut", "farm.query.vgi.examples.TableInOutExample")
registerWorker("runAggregate", "farm.query.vgi.examples.AggregateExample")
registerWorker("runBuffering", "farm.query.vgi.examples.BufferingExample")
registerWorker("runCatalog", "farm.query.vgi.examples.CatalogExample")
