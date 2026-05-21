package com.hiczp.openai.stream.proxy

import io.ktor.sse.*

private const val RESOURCE_DIR = "com/hiczp/openai/stream/proxy"

internal fun sseEvents(fileName: String): List<ServerSentEvent> {
    val resourcePath = "$RESOURCE_DIR/$fileName"
    val text = checkNotNull(Thread.currentThread().contextClassLoader.getResource(resourcePath)) {
        "Missing test resource: $resourcePath"
    }.readText()

    return text
        .replace("\r\n", "\n")
        .replace('\r', '\n')
        .trim()
        .split(Regex("\n{2,}"))
        .map { block ->
            val lines = block.lines()
            ServerSentEvent(
                event = lines.firstNotNullOfOrNull { it.valueAfter("event:") },
                data = lines.mapNotNull { it.valueAfter("data:") }.joinToString("\n").ifEmpty { null },
            )
        }
}

private fun String.valueAfter(prefix: String): String? =
    takeIf { it.startsWith(prefix) }?.removePrefix(prefix)?.removePrefix(" ")
