package com.hiczp.openai.responses.stream.proxy.cli

actual fun exitProcess(statusCode: Int): Nothing = kotlin.system.exitProcess(statusCode)
