// Copyright 2026 Query Farm LLC - https://query.farm
plugins {
    `java-library`
}

dependencies {
    // farm.query is the published group (see ../build.gradle.kts allprojects).
    // RPC 0.33.2 preserves declared scalar dictionary fields in catalog records.
    // TODO(release-wave): switch to the vgi-rpc-java release that ships
    // farm.query.vgirpc.vendor (blob vendor) and upload-URL protocol v2
    // (VGI-Upload-URL-Support: 2, vgi protocol 3.4). Until then CI builds vgirpc from
    // source at VGI_RPC_JAVA_REF through the composite build (VGI_RPC_JAVA_DIR).
    api("farm.query:vgirpc:0.33.2")
    // Orchard hosted mode (OrchardHosted): the hardened JwtAuthenticator verifies Passes.
    // Built from source with :vgirpc through the composite build.
    implementation("farm.query:vgirpc-oauth:0.33.2")
    // OrchardHosted wraps vgirpc's Authenticator, which takes a servlet request; Jetty supplies
    // the API at runtime through vgirpc.
    compileOnly("jakarta.servlet:jakarta.servlet-api:6.0.0")
    implementation("org.slf4j:slf4j-api:2.0.17")
    // Cross-process aggregate state store. DuckDB spawns multiple worker
    // subprocesses for parallel aggregation; SQLite's file locking gives us
    // a shared backing store with the same semantics as vgi-go.
    implementation("org.xerial:sqlite-jdbc:3.49.1.0")
    // JSON for the Cloudflare Durable Object storage client's HTTP bodies.
    implementation("com.google.code.gson:gson:2.14.0")
    implementation("com.github.luben:zstd-jni:1.5.6-8")

    testImplementation("org.junit.jupiter:junit-jupiter:5.12.2")
    // GrantBearerHttpTest's login authenticator reads the request (vgirpc's
    // Authenticator takes a servlet request); Jetty supplies it at runtime.
    testCompileOnly("jakarta.servlet:jakarta.servlet-api:6.0.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
