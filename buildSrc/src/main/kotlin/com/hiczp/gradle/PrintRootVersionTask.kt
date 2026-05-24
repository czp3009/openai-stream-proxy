package com.hiczp.gradle

import org.gradle.api.DefaultTask
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction

abstract class PrintRootVersionTask : DefaultTask() {
    @get:Input
    abstract val rootVersion: Property<String>

    @TaskAction
    fun printRootVersion() {
        println(rootVersion.get())
    }
}
