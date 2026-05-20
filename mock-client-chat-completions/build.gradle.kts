import org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi

plugins {
    alias(libs.plugins.kotlinMultiplatform)
}

kotlin {
    jvm {
        @OptIn(ExperimentalKotlinGradlePluginApi::class)
        binaries {
            executable {
                mainClass.set("com.hiczp.openai.stream.proxy.mockclient.MainKt")
            }
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation(libs.kotlin.logging)
            implementation(libs.logback.classic)
            implementation(libs.openai.java)
            implementation(libs.openai.java.okhttp)
        }
    }
}
