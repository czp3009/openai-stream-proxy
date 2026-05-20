package com.hiczp.openai.responses.stream.proxy.cli

import ch.qos.logback.classic.Level
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ch.qos.logback.classic.Logger as LogbackLogger

actual fun configureLogging() {
    (LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as LogbackLogger).level = Level.INFO
}
