package com.hiczp.openai.stream.proxy.cli

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.staticCFunction
import platform.posix.SIGINT
import platform.posix.SIGTERM
import platform.posix.signal

private var shutdownBlock: (() -> Unit)? = null

@OptIn(ExperimentalForeignApi::class)
actual fun registerShutdownHook(block: () -> Unit) {
    shutdownBlock = block
    val handler = staticCFunction<Int, Unit> {
        shutdownBlock?.invoke()
    }
    signal(SIGTERM, handler)
    signal(SIGINT, handler)
}
