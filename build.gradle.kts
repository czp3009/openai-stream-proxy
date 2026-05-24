import com.hiczp.gradle.PrintRootVersionTask
import org.jetbrains.kotlin.gradle.dsl.KotlinMultiplatformExtension
import org.jetbrains.kotlin.gradle.plugin.KotlinMultiplatformPluginWrapper

plugins {
    alias(libs.plugins.kotlinMultiplatform) apply false
    alias(libs.plugins.kotlinSerialization) apply false
}

group = "com.hiczp"
version = "0.0.1"

subprojects {
    group = rootProject.group
    version = rootProject.version

    plugins.withType<KotlinMultiplatformPluginWrapper> {
        extensions.configure<KotlinMultiplatformExtension> {
            jvmToolchain(21)
        }
    }
}

tasks.register<PrintRootVersionTask>("printRootVersion") {
    group = "help"
    description = "Prints rootProject.version only."
    rootVersion.set(rootProject.version.toString())
}
