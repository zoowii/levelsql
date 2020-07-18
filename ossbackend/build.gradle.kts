plugins {
    java
    kotlin("jvm")
    maven
}

val parentVersion = "1.0.0-dev"

group = "com.zoowii"
version = parentVersion

repositories {
    mavenLocal()
    mavenCentral()
}

dependencies {
    compile("com.zoowii", "levelsqlcore", parentVersion)

    implementation(kotlin("stdlib-jdk8"))
    testCompile("junit", "junit", "4.12")

    compile("com.qcloud", "cos_api", "5.6.24")
    compile("com.mashape.unirest", "unirest-java", "1.4.9")
}

configure<JavaPluginConvention> {
    sourceCompatibility = JavaVersion.VERSION_1_8
}
tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = "1.8"
    }
}