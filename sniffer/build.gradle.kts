plugins {
    alias(libs.plugins.kotlinMultiplatform)
}

kotlin {
    jvm {
        @OptIn(org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi::class)
        binaries {
            executable {
                mainClass.set("com.hiczp.openai.responses.stream.proxy.sniffer.MainKt")
            }
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation(libs.ktor.server.core)
            implementation(libs.ktor.server.cio)
            implementation(libs.ktor.client.core)
            implementation(libs.ktor.client.cio)
            implementation(libs.ktor.http)
            implementation(libs.ktor.io)
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlin.logging)
            implementation(libs.logback.classic)
        }
    }
}
