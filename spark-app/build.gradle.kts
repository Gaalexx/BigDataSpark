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

    implementation("org.apache.spark:spark-core_2.12:3.5.6")
    implementation("org.apache.spark:spark-sql_2.12:3.5.6")

    implementation("org.postgresql:postgresql:42.7.7")
    implementation("com.clickhouse:clickhouse-jdbc:0.9.1")
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}