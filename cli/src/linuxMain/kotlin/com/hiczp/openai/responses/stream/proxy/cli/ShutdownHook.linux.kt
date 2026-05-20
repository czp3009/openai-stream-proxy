package com.hiczp.openai.responses.stream.proxy.cli

import kotlinx.cinterop.COpaquePointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.staticCFunction
import platform.posix.*

private var shutdownCallback: (() -> Unit)? = null

@OptIn(ExperimentalForeignApi::class)
actual fun registerShutdownHook(block: () -> Unit) {
    shutdownCallback = block
    val handler = staticCFunction<Int, Unit> {
        pthread_create(
            null, null,
            staticCFunction<COpaquePointer?, COpaquePointer?> {
                try {
                    shutdownCallback?.invoke()
                } finally {
                    exit(0)
                }
                @Suppress("UNREACHABLE_CODE")
                null
            },
            null,
        )
    }
    signal(SIGTERM, handler)
    signal(SIGINT, handler)
}
