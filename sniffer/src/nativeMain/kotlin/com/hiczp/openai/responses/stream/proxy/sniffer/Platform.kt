package com.hiczp.openai.responses.stream.proxy.sniffer

import kotlinx.cinterop.*
import platform.posix.*

@OptIn(ExperimentalForeignApi::class)
internal actual fun environment(name: String): String? {
    return getenv(name)?.toKString()
}
