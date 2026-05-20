package com.hiczp.openai.responses.stream.proxy.cli

actual fun registerShutdownHook(block: () -> Unit) {
    Runtime.getRuntime().addShutdownHook(Thread(block))
}
