package com.hiczp.openai.responses.stream.proxy.cli

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.staticCFunction
import platform.posix.SIGINT
import platform.posix.SIGTERM
import platform.posix.signal

private var shutdownCallback: (() -> Unit)? = null

@OptIn(ExperimentalForeignApi::class)
actual fun registerShutdownHook(block: () -> Unit) {
    shutdownCallback = block
    signal(SIGTERM, staticCFunction<Int, Unit> { shutdownCallback?.invoke() })
    signal(SIGINT, staticCFunction<Int, Unit> { shutdownCallback?.invoke() })
}
