plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.kotlinSerialization)
}

kotlin {
    jvm()
    mingwX64()
    linuxX64()
    macosArm64()

    sourceSets {
        commonMain.dependencies {
            implementation(project(":proxy"))
            implementation(libs.ktor.server.core)
            implementation(libs.ktor.server.cio)
            implementation(libs.ktor.server.status.pages)
            implementation(libs.ktor.client.cio)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.cli)
            implementation(libs.kotlin.logging)
        }
        jvmMain.dependencies {
            implementation(libs.logback.classic)
        }
        jvmTest.dependencies {
            implementation(libs.ktor.server.cio)
            implementation(libs.ktor.server.status.pages)
            implementation(libs.ktor.client.cio)
            implementation(libs.logback.classic)
            implementation(kotlin("test"))
        }
    }
}
