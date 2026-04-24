package com.hiczp.openai.responses.stream.proxy.sniffer

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.request.*
import io.ktor.utils.io.*

private val logger = KotlinLogging.logger("TrafficLogger")

object TrafficLogger {
    // Log incoming request headers.
    fun logRequest(method: HttpMethod, uri: String, headers: Headers) {
        logger.info { ">>> ${method.value} $uri" }
        logHeaders(">>>", headers)
    }

    // Log incoming request body chunk.
    fun logRequestBodyChunk(bytes: ByteArray) {
        logger.info { bytes.decodeToString() }
    }

    // Log upstream response headers.
    fun logResponse(status: HttpStatusCode, headers: Headers) {
        logger.info { "<<< ${status.value} ${status.description}" }
        logHeaders("<<<", headers)
    }

    // Log upstream response body chunk.
    fun logResponseBodyChunk(bytes: ByteArray) {
        logger.info { bytes.decodeToString() }
    }

    private fun logHeaders(prefix: String, headers: Headers) {
        headers.forEach { name, values ->
            for (value in values) {
                logger.info { "$prefix $name: $value" }
            }
        }
    }
}
