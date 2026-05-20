package com.hiczp.openai.stream.proxy.sniffer

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.*
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.writeString

private val logger = KotlinLogging.logger {}

object TrafficLogger {
    fun logHeaders(prefix: String, headers: Headers) {
        headers.forEach { name, values ->
            for (value in values) {
                logger.info { "$prefix $name: $value" }
            }
        }
    }

    fun logRequest(method: HttpMethod, version: String, uri: String, headers: Headers, body: ByteArray) {
        logger.info { ">>> ${method.value} $uri" }
        logHeaders(">>>", headers)
        if (body.isNotEmpty()) {
            logger.info { body.decodeToString() }
        }

        val dumpPath = resolveDumpPath(uri)
        dumpPath.parent?.let { SystemFileSystem.createDirectories(it) }
        SystemFileSystem.sink(dumpPath, append = true).buffered().use { sink ->
            sink.writeString(buildString {
                appendLine("${method.value} $uri $version")
                headers.forEach { name, values ->
                    values.forEach { appendLine("$name: $it") }
                }
                appendLine()
                if (body.isNotEmpty()) append(body.decodeToString())
                appendLine("\n\n")
            })
        }
    }

    fun logResponse(
        uri: String,
        version: HttpProtocolVersion,
        status: HttpStatusCode,
        headers: Headers,
        body: ByteArray
    ) {
        logger.info { "<<< ${status.value} ${status.description}" }
        logHeaders("<<<", headers)
        if (body.isNotEmpty()) {
            logger.info { body.decodeToString() }
        }

        val dumpPath = resolveDumpPath(uri)
        dumpPath.parent?.let { SystemFileSystem.createDirectories(it) }
        SystemFileSystem.sink(dumpPath, append = true).buffered().use { sink ->
            sink.writeString(buildString {
                appendLine("$version ${status.value} ${status.description}")
                headers.forEach { name, values ->
                    values.forEach { appendLine("$name: $it") }
                }
                appendLine()
                if (body.isNotEmpty()) append(body.decodeToString())
                appendLine("\n\n")
            })
        }
    }

    private fun resolveDumpPath(uri: String): Path {
        val path = uri.substringBefore("?")
        return when {
            path.endsWith("/responses") -> Path("temp/responses.txt")
            path.endsWith("/chat/completions") -> Path("temp/chat_completions.txt")
            else -> Path("temp/others.txt")
        }
    }
}
