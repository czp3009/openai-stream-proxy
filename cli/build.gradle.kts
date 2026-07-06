import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar.Companion.shadowJar
import org.jetbrains.kotlin.gradle.ExperimentalKotlinGradlePluginApi

plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.kotlinSerialization)
    alias(libs.plugins.shadow)
    alias(libs.plugins.kotlinNativeNpmPublishing)
}

val nativeEntryPoint = "com.hiczp.openai.stream.proxy.cli.main"
val jvmMainClass = "com.hiczp.openai.stream.proxy.cli.MainKt"

kotlin {
    jvm {
        @OptIn(ExperimentalKotlinGradlePluginApi::class)
        binaries {
            executable {
                this.mainClass.set(jvmMainClass)
            }
        }
    }
    mingwX64 {
        binaries.executable {
            this.entryPoint = nativeEntryPoint
            this.baseName = "${rootProject.name}-${rootProject.version}-${this@mingwX64.name}"
        }
    }
    linuxX64 {
        binaries.executable {
            this.entryPoint = nativeEntryPoint
            this.baseName = "${rootProject.name}-${rootProject.version}-${this@linuxX64.name}"
        }
    }
    macosArm64 {
        binaries.executable {
            this.entryPoint = nativeEntryPoint
            this.baseName = "${rootProject.name}-${rootProject.version}-${this@macosArm64.name}"
        }
    }

    sourceSets {
        commonMain.dependencies {
            implementation(project(":proxy"))
            implementation(libs.ktor.server.core)
            implementation(libs.ktor.server.cio)
            implementation(libs.ktor.server.status.pages)
            implementation(libs.ktor.server.websockets)
            implementation(libs.ktor.client.websockets)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.kotlinx.cli)
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
        jvmTest.dependencies {
            implementation(kotlin("test"))
        }
    }
}

tasks.named<Jar>("jvmJar") {
    this.archiveBaseName = rootProject.name
    this.archiveAppendix = ""
}

tasks.shadowJar {
    this.archiveBaseName = rootProject.name
    this.archiveAppendix = ""
    this.archiveVersion = rootProject.version as String
    this.archiveClassifier = "fat"
    this.manifest {
        this.attributes["Main-Class"] = jvmMainClass
    }
}

kotlinNativeNpmPublishing {
    packageName.set("@czp3009/openai-stream-proxy")
    description.set("Convert OpenAI Responses/ChatCompletions API request from non-stream mode to stream mode")
    license.set("MIT")
    repository.set("https://github.com/czp3009/openai-stream-proxy")
    homepage.set("https://github.com/czp3009/openai-stream-proxy")
    keywords.addAll("openai", "openai-responses", "proxy", "kotlin-native", "npm", "npx")
    access.set("public")
    otp.set(providers.gradleProperty("npmOtp"))

    stage {
        main {
            readme()
            license(rootProject.layout.projectDirectory.file("LICENSE.txt"))
        }

        platforms {
            license(rootProject.layout.projectDirectory.file("LICENSE.txt"))
        }
    }
}
