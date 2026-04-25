import org.jetbrains.kotlin.gradle.plugin.mpp.KotlinNativeTarget

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
    mingwX64()
    linuxX64()
    linuxArm64()
    macosArm64()

    targets.withType<KotlinNativeTarget> {
        binaries.executable {
            entryPoint = "com.hiczp.openai.responses.stream.proxy.sniffer.main"
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation(libs.ktor.server.core)
            implementation(libs.ktor.server.cio)
            implementation(libs.ktor.client.core)
            implementation(libs.ktor.http)
            implementation(libs.ktor.io)
            implementation(libs.kotlinx.coroutines.core)
            implementation(libs.kotlin.logging)
        }
        jvmMain.dependencies {
            implementation(libs.ktor.client.cio)
            implementation(libs.logback.classic)
        }
        nativeMain.dependencies {
            implementation(libs.ktor.client.curl)
        }
    }
}
