package com.hiczp.openai.responses.stream.proxy.sniffer

internal actual fun environment(name: String): String? = System.getenv(name)
