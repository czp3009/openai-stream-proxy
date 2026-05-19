package com.hiczp.openai.responses.stream.proxy.sniffer

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.*
import kotlinx.io.buffered
import kotlinx.io.files.Path
import kotlinx.io.files.SystemFileSystem
import kotlinx.io.writeString

private val logger = KotlinLogging.logger {}
private val dumpPath = Path("temp/sniffer.txt")

object TrafficLogger {
    fun logRequest(method: HttpMethod, version: String, uri: String, headers: Headers, body: ByteArray) {
        logger.info { ">>> ${method.value} $uri" }
        logHeaders(">>>", headers)
        if (body.isNotEmpty()) {
            logger.info { body.decodeToString() }
        }

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

    fun logResponse(version: HttpProtocolVersion, status: HttpStatusCode, headers: Headers, body: ByteArray) {
        logger.info { "<<< ${status.value} ${status.description}" }
        logHeaders("<<<", headers)
        if (body.isNotEmpty()) {
            logger.info { body.decodeToString() }
        }

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

    private fun logHeaders(prefix: String, headers: Headers) {
        headers.forEach { name, values ->
            for (value in values) {
                logger.info { "$prefix $name: $value" }
            }
        }
    }
}
