package com.hiczp.openai.stream.proxy.cli

/**
 * Registers [block] to run on process termination (SIGTERM / SIGINT / JVM shutdown).
 *
 * **JVM:** Wraps [block] in a new [Thread] and registers it via [Runtime.addShutdownHook].
 *
 * **Native:** Installs a POSIX signal handler that invokes [block] on the main thread.
 */
expect fun registerShutdownHook(block: () -> Unit)
