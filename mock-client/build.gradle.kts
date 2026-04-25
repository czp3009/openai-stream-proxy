plugins {
    alias(libs.plugins.kotlinMultiplatform)
}

kotlin {
    jvm {
        @OptIn(org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi::class)
        binaries {
            executable {
                mainClass.set("com.hiczp.openai.responses.stream.proxy.mockclient.MainKt")
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
