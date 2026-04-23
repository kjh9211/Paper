plugins {
    application
}

dependencies {
    implementation("redis.clients:jedis:5.2.0")
    implementation("org.yaml:snakeyaml:2.2")

    runtimeOnly("com.h2database:h2:2.3.232")
    runtimeOnly("com.mysql:mysql-connector-j:9.2.0")
}

application {
    mainClass.set("com.aether.orchestrator.AetherLauncher")
}
