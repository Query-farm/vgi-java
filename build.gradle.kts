plugins {
    java
}

allprojects {
    group = "farm.query"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")
    apply(plugin = "maven-publish")

    extensions.configure<PublishingExtension> {
        publications {
            create<MavenPublication>("maven") {
                from(components["java"])
            }
        }
    }

    extensions.configure<JavaPluginExtension> {
        // Build with JDK 25 but target Java 21 bytecode so the example worker
        // runs on any JDK >= 21. The shared-memory transport (java.lang.foreign,
        // GA in 22) lives in vgi-rpc-java's Java 22 multi-release overlay and
        // activates only on JDK >= 22; on 21 the worker uses the pipe transport.
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.release.set(21)
        options.compilerArgs.addAll(
            listOf(
                "-Xlint:all,-serial,-processing",
                // Required: ParameterBinder reads Parameter.getName() for kwargs binding.
                "-parameters",
            )
        )
        options.encoding = "UTF-8"
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        // Arrow's memory module needs access to java.nio internals;
        // FFM (shm_open/mmap) needs native access without warnings.
        jvmArgs(
            "--add-opens=java.base/java.nio=ALL-UNNAMED",
            "--enable-native-access=ALL-UNNAMED",
        )
        // Surface Arrow allocator leaks immediately.
        systemProperty("arrow.memory.debug.allocator", "true")
    }
}
