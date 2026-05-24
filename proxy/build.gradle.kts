plugins {
    alias(libs.plugins.kotlinMultiplatform)
    alias(libs.plugins.kotlinSerialization)
    alias(libs.plugins.mavenPublish)
}

kotlin {
    jvm()
    mingwX64()
    linuxX64()
    linuxArm64()
    macosArm64()

    sourceSets {
        commonMain.dependencies {
            api(libs.ktor.client.core)
            api(libs.ktor.http)
            api(libs.ktor.io)
            api(libs.kotlinx.serialization.json)
            implementation(libs.kotlin.logging)
        }

        commonTest.dependencies {
            implementation(kotlin("test"))
        }

        jvmTest.dependencies {
            implementation(libs.ktor.server.core)
            implementation(libs.ktor.server.cio)
            implementation(libs.ktor.client.cio)
            implementation(libs.logback.classic)
        }
    }
}

mavenPublishing {
    coordinates(project.group.toString(), "openai-stream-proxy", project.version.toString())
    publishToMavenCentral()
    signAllPublications()

    pom {
        name = "openai-stream-proxy"
        description =
            "A Kotlin Multiplatform library for proxying non-streaming OpenAI API requests through upstream SSE streams"
        inceptionYear = "2026"
        url = "https://github.com/czp3009/openai-stream-proxy"
        licenses {
            license {
                name = "MIT"
                url = "https://opensource.org/licenses/MIT"
                distribution = "https://opensource.org/licenses/MIT"
            }
        }
        developers {
            developer {
                id = "czp3009"
                name = "czp3009"
                email = "czp3009@gmail.com"
                url = "https://github.com/czp3009"
            }
        }
        scm {
            url = "https://github.com/czp3009/openai-stream-proxy"
            connection = "scm:git:git://github.com/czp3009/openai-stream-proxy.git"
            developerConnection = "scm:git:ssh://git@github.com/czp3009/openai-stream-proxy.git"
        }
    }
}
