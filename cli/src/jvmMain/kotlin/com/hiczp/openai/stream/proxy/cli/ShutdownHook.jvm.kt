package com.hiczp.openai.stream.proxy.cli

actual fun registerShutdownHook(block: () -> Unit) {
    Runtime.getRuntime().addShutdownHook(Thread(block))
}
