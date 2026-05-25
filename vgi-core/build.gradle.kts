plugins {
    `java-library`
}

dependencies {
    // farm.query is the published group (see ../build.gradle.kts allprojects).
    api("farm.query:vgirpc:0.1.0-SNAPSHOT")
    implementation("org.slf4j:slf4j-api:2.0.16")
    // Cross-process aggregate state store. DuckDB spawns multiple worker
    // subprocesses for parallel aggregation; SQLite's file locking gives us
    // a shared backing store with the same semantics as vgi-go.
    implementation("org.xerial:sqlite-jdbc:3.46.1.3")
    // JSON for the Cloudflare Durable Object storage client's HTTP bodies.
    implementation("com.google.code.gson:gson:2.11.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
