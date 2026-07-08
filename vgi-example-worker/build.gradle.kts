// Copyright 2026 Query Farm LLC - https://query.farm
plugins {
    application
}

dependencies {
    implementation(project(":vgi"))
    implementation("org.slf4j:slf4j-simple:2.0.16")
    // Embedded Haybarn engine for evaluating pushed expression filters
    // (spatial &&, list_contains, ...) against emitted batches — mirrors
    // vgi-python's `vgi._duckdb` expression-filter evaluator. arrow-c-data
    // bridges an Arrow batch into the engine via the C Data interface.
    implementation("farm.query.haybarn:haybarn_jdbc:1.5.4-rc1")
    implementation("org.apache.arrow:arrow-c-data:18.1.0")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("farm.query.vgi.example.Main")
    // ParameterBinder reads Parameter.getName() — required for kwargs binding.
    // (Already covered by -parameters compile flag in root build, but the
    // Arrow allocator opens are needed at runtime too.)
    applicationDefaultJvmArgs = listOf(
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        // FFM shm_open/mmap downcalls run in the worker JVM.
        "--enable-native-access=ALL-UNNAMED",
    )
}
