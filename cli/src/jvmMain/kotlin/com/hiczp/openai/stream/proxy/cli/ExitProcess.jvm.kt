package com.hiczp.openai.stream.proxy.cli

actual fun exitProcess(statusCode: Int): Nothing = kotlin.system.exitProcess(statusCode)
