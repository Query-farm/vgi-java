// Copyright 2026 Query Farm LLC - https://query.farm
plugins {
    java
    // Applied (below) only to the published library module. Targets the
    // Sonatype Central Portal by default and handles signing + sources/javadoc.
    id("com.vanniktech.maven.publish") version "0.36.0" apply false
}

allprojects {
    group = "farm.query"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}

// Library modules published to Maven Central. vgi-example-worker is the
// runnable example/test worker (the integration-suite fixtures) and is
// intentionally NOT published.
val publishedModules = setOf("vgi")

val moduleDescriptions = mapOf(
    "vgi" to "Java implementation of the VGI (Vector Gateway Interface) protocol " +
        "for serving DuckDB catalog data to external workers over Apache Arrow IPC.",
)

subprojects {
    apply(plugin = "java")

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

    if (name in publishedModules) {
        apply(plugin = "com.vanniktech.maven.publish")

        // The codebase predates strict doc-comment hygiene; don't fail the
        // build (and thus publishing) on doclint warnings.
        tasks.withType<Javadoc>().configureEach {
            (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
        }

        extensions.configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
            // Upload to the Central Portal and auto-release once validation
            // passes (no manual "publish" click in the Portal UI). Flip to
            // publishToMavenCentral(false) if you'd rather gate releases by hand.
            publishToMavenCentral(automaticRelease = true)

            // Sign only when a key is available (CI / local release). Plain
            // `publishToMavenLocal` and contributor builds without a key still work.
            if (project.findProperty("signingInMemoryKey") != null) {
                signAllPublications()
            }

            coordinates(group.toString(), name, version.toString())

            pom {
                name.set(this@subprojects.name)
                description.set(
                    moduleDescriptions[this@subprojects.name]
                        ?: "vgi-java module ${this@subprojects.name}."
                )
                url.set("https://github.com/Query-farm/vgi-java")
                inceptionYear.set("2026")
                licenses {
                    license {
                        name.set("Query Farm Source-Available License, Version 1.0")
                        url.set("https://github.com/Query-farm/vgi-java/blob/main/LICENSE")
                    }
                }
                organization {
                    name.set("Query Farm LLC")
                    url.set("https://query.farm")
                }
                developers {
                    developer {
                        name.set("Query Farm LLC")
                        url.set("https://query.farm")
                    }
                }
                scm {
                    connection.set("scm:git:https://github.com/Query-farm/vgi-java.git")
                    developerConnection.set("scm:git:git@github.com:Query-farm/vgi-java.git")
                    url.set("https://github.com/Query-farm/vgi-java")
                }
            }
        }
    }
}
