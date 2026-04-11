plugins {
    kotlin("jvm") version "2.2.21"
}

group = "lab2.lab"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    implementation(kotlin("stdlib"))

    compileOnly("org.apache.spark:spark-core_2.12:3.5.6")
    compileOnly("org.apache.spark:spark-sql_2.12:3.5.6")

    implementation("org.postgresql:postgresql:42.7.7")
    implementation("com.clickhouse:clickhouse-jdbc:0.9.1")
}

kotlin {
    jvmToolchain(11)
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_11)
    }
}

tasks.test {
    useJUnitPlatform()
}

tasks.register<Jar>("fatJar") {
    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    from(sourceSets.main.get().output)
    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith(".jar") }
            .map { zipTree(it) }
    })
    exclude("META-INF/*.DSA", "META-INF/*.RSA", "META-INF/*.SF")
}

tasks.build {
    dependsOn("fatJar")
}
