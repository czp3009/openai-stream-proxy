package com.hiczp.openai.stream.proxy.cli

import ch.qos.logback.classic.Level
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Logger as LogbackLogger

actual fun configureLogging() {
    val level = System.getenv("LOG_LEVEL")?.uppercase()?.let {
        Level.toLevel(it, Level.INFO)
    } ?: Level.INFO
    (LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as LogbackLogger).level = level
}
