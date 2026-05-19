package com.hiczp.openai.responses.stream.proxy.cli

/**
 * Registers [block] to run on process termination (SIGTERM / SIGINT / JVM shutdown).
 *
 * **JVM:** Wraps [block] in a new [Thread] and registers it via [Runtime.addShutdownHook].
 * The JVM shutdown sequence runs all hooks concurrently; the process exits after they complete.
 *
 * **Native:** Registers a C signal handler for SIGTERM and SIGINT via `signal(2)`.
 * Because signal handlers run in a restricted context (on Windows, a separate console-handler thread;
 * on POSIX, the main thread is interrupted), [block] must not be called directly — it would deadlock
 * (e.g. `server.stop()` joining the interrupted main thread) or fail to dispatch coroutines.
 * Instead the handler spawns a new `pthread` that runs [block], then calls `exit(0)` to terminate
 * the process (flushing stdio buffers, unlike `_exit`).
 *
 * The native `actual` is placed in per-platform source sets (`mingwX64Main`, `linuxX64Main`,
 * `macosArm64Main`) rather than `nativeMain` because `platform.posix.pthread_create` is not
 * available in the commonized POSIX declarations during metadata compilation.
 */
expect fun registerShutdownHook(block: () -> Unit)
