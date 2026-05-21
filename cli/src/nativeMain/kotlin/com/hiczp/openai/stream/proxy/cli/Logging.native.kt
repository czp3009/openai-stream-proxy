package com.hiczp.openai.stream.proxy.cli

import io.github.oshai.kotlinlogging.KotlinLoggingConfiguration
import io.github.oshai.kotlinlogging.Level
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.toKString
import platform.posix.getenv

@OptIn(ExperimentalForeignApi::class)
actual fun configureLogging() {
    val level = getenv("LOG_LEVEL")?.toKString()?.uppercase()?.let {
        runCatching { Level.valueOf(it) }.getOrNull()
    } ?: Level.DEBUG
    KotlinLoggingConfiguration.direct.logLevel = level
}
