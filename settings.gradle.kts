rootProject.name = "openai-responses-stream-proxy"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        google()
    }
}

dependencyResolutionManagement {
    @Suppress("UnstableApiUsage")
    repositories {
        mavenCentral()
        google()
    }
}

include(":proxy")
include(":cli")
include(":sniffer")
include(":mock-client")
