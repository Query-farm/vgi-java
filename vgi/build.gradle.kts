// Copyright 2026 Query Farm LLC - https://query.farm
plugins {
    `java-library`
}

dependencies {
    // farm.query is the published group (see ../build.gradle.kts allprojects).
    // RPC 0.34.0: farm.query.vgirpc.vendor (blob vendor), upload-URL protocol v2
    // (VGI-Upload-URL-Support: 2, vgi protocol 3.4) and the hardened JwtAuthenticator.
    api("farm.query:vgirpc:0.34.0")
    // Orchard hosted mode (OrchardHosted): the hardened JwtAuthenticator verifies Passes.
    implementation("farm.query:vgirpc-oauth:0.34.0")
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
    // Direct object storage (VGI_STORAGE_BACKEND=s3|gcs): compile against the backends, but a
    // worker carries the AWS / Google SDKs only if it adds the module it uses (StorageBackend
    // loads them on demand and names the missing module otherwise).
    compileOnly("farm.query:vgirpc-s3:0.34.0")
    compileOnly("farm.query:vgirpc-gcs:0.34.0")
    testImplementation("farm.query:vgirpc-s3:0.34.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.12.2")
    // GrantBearerHttpTest's login authenticator reads the request (vgirpc's
    // Authenticator takes a servlet request); Jetty supplies it at runtime.
    testCompileOnly("jakarta.servlet:jakarta.servlet-api:6.0.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
