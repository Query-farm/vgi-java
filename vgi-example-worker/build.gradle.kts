plugins {
    application
}

dependencies {
    implementation(project(":vgi-core"))
    implementation("org.slf4j:slf4j-simple:2.0.16")
}

application {
    mainClass.set("farm.query.vgi.example.Main")
    // ParameterBinder reads Parameter.getName() — required for kwargs binding.
    // (Already covered by -parameters compile flag in root build, but the
    // Arrow allocator opens are needed at runtime too.)
    applicationDefaultJvmArgs = listOf("--add-opens=java.base/java.nio=ALL-UNNAMED")
}
