import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    java
    kotlin("jvm") version "1.2.51"
}

group = "com.zoowii"
version = "1.0.0-dev"

repositories {
    mavenCentral()
}

dependencies {
    compile(kotlin("stdlib-jdk8"))
    testCompile("junit", "junit", "4.12")
    testCompile("org.jetbrains.kotlin", "kotlin-test", "1.3.50")

    compile("org.iq80.leveldb", "leveldb", "0.12")
    compile("com.google.guava", "guava", "28.1-jre")
    compile("com.alibaba", "fastjson", "1.2.60")

    // slf4j
    compile("org.slf4j", "slf4j-api", "1.8.0-beta4")
    testCompile("org.slf4j", "slf4j-simple", "1.8.0-beta4")

    // google cloud
    compile("com.google.cloud", "google-cloud-storage", "1.100.0")

}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}
tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}