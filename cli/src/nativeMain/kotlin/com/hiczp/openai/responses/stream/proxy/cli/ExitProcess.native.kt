package com.hiczp.openai.responses.stream.proxy.cli

import platform.posix.exit

actual fun exitProcess(statusCode: Int): Nothing {
    exit(statusCode)
    throw IllegalStateException()
}
