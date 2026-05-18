package com.hiczp.openai.responses.stream.proxy.cli

import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.readString
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

@Serializable
data class ProxyRule(
    val listenPort: Int,
    val upstreamUrl: String,
)

private val json = Json { ignoreUnknownKeys = true }

fun loadConfig(path: String): List<ProxyRule> {
    val text = SystemFileSystem.source(Path(path)).buffered().use { it.readString() }
    return json.decodeFromString<List<ProxyRule>>(text)
}
