plugins {
    kotlin("jvm") version "2.3.21"
}

group = "com.yasashny"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(24)
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    archiveFileName.set("raft.jar")
    manifest {
        attributes["Main-Class"] = "com.yasashny.raft.CliMainKt"
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith("jar") }
            .map { zipTree(it) }
    })
}
