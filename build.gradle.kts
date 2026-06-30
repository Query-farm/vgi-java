// Copyright 2026 Query Farm LLC - https://query.farm
plugins {
    java
    // Applied (below) only to the published library module. Targets the
    // Sonatype Central Portal by default and handles signing + sources/javadoc.
    id("com.vanniktech.maven.publish") version "0.36.0" apply false
}

allprojects {
    group = "farm.query"
    version = "0.10.0"

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
        "for serving catalog data to Haybarn, Query Farm's DuckDB-derived engine, " +
        "from external workers over Apache Arrow IPC.",
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

        tasks.withType<Javadoc>().configureEach {
            val logo = rootProject.layout.projectDirectory.file("assets/vgi-logo.png").asFile
            val brandCss = file("src/main/javadoc/query-farm.css")
            val overviewHtml = file("src/main/javadoc/overview.html")
            inputs.file(logo)
            inputs.file(brandCss)
            inputs.file(overviewHtml)
            with(options as StandardJavadocDocletOptions) {
                addBooleanOption("Xdoclint:all", true)
                addStringOption("Xmaxwarns", "10000")
                windowTitle = "${project.name} $version — Query.Farm VGI for Java"
                docTitle = "<img src='vgi-logo.png' alt='Vector Gateway Interface' class='qf-logo'>" +
                    "${project.name} $version API"
                header = "<a href='https://query.farm' target='_top'>🚜 Query.Farm</a>"
                bottom = "Copyright © 2026 <a href='https://query.farm'>Query Farm LLC</a> · " +
                    "<a href='https://github.com/Query-farm/vgi-java'>github.com/Query-farm/vgi-java</a> · " +
                    "Query Farm Source-Available License"
                overview = overviewHtml.path
                addStringOption("-add-stylesheet", brandCss.path)
            }
            // The doctitle's logo isn't a doclet-managed resource; drop it into
            // the output root so the javadoc jar stays self-contained.
            doLast {
                logo.copyTo(destinationDir!!.resolve("vgi-logo.png"), overwrite = true)
            }
        }

        tasks.named<Jar>("jar") {
            manifest {
                attributes(
                    // Stable JPMS module name for consumers on the module path;
                    // without it the name is derived from the jar filename.
                    "Automatic-Module-Name" to "farm.query.${project.name.replace("-", ".")}",
                    "Implementation-Title" to "${project.group}:${project.name}",
                    "Implementation-Version" to version,
                    "Implementation-Vendor" to "Query Farm LLC",
                )
            }
        }

        extensions.configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
            // Auto-release to Maven Central: once the upload passes Portal
            // validation the deployment is published without a manual "Publish"
            // click. (The coordinate has been through validation since 0.1.0;
            // set this back to false to re-gate on the Portal UI.)
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
