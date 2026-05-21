package com.hiczp.openai.stream.proxy

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.sse.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.util.*
import io.ktor.utils.io.*
import kotlinx.io.IOException

private val logger = KotlinLogging.logger("com.hiczp.openai.stream.proxy.ApiProxy")

abstract class AbstractApiProxy(
    val engine: HttpClientEngine,
    val upstreamBaseUrl: String,
    val timeoutMillis: Long = 600_000L,
) {
    protected val client = HttpClient(engine) {
        install(SSE)
        install(HttpTimeout) {
            this.requestTimeoutMillis = timeoutMillis
            this.connectTimeoutMillis = 10_000
            this.socketTimeoutMillis = timeoutMillis
        }
    }

    abstract suspend fun proxy(
        requestMethod: HttpMethod,
        requestUri: String,
        requestHeaders: Headers,
        requestBody: ByteReadChannel,
    ): OutgoingContent?

    protected suspend fun passthrough(
        upstreamUrl: String,
        method: HttpMethod,
        headers: Headers,
        body: ByteReadChannel,
    ): OutgoingContent? {
        val forwardHeaders = stripHopByHopHeaders(headers)

        val upstreamResponse = try {
            client.request(upstreamUrl) {
                this.method = method
                this.headers.appendAll(forwardHeaders)
                this.setBody(body)
            }
        } catch (e: IOException) {
            logger.warn { "Passthrough request to $upstreamUrl failed: ${e.message}" }
            return null
        }

        val filteredHeaders = upstreamResponse.headers.filter { key, _ ->
            !key.equals(HttpHeaders.TransferEncoding, ignoreCase = true)
        }

        val bodyChannel = upstreamResponse.bodyAsChannel()
        return object : OutgoingContent.ReadChannelContent() {
            override val contentLength = upstreamResponse.contentLength()
            override val status = upstreamResponse.status
            override val headers = Headers.build { appendAll(filteredHeaders) }
            override fun readFrom(): ByteReadChannel = bodyChannel
        }
    }

    protected suspend fun passthrough(
        upstreamUrl: String,
        method: HttpMethod,
        headers: Headers,
        bodyBytes: ByteArray,
    ): OutgoingContent? = passthrough(upstreamUrl, method, headers, ByteReadChannel(bodyBytes))

    companion object {
        private val hopByHopHeaderNames = setOf(
            "host",
            "content-length",
            "transfer-encoding",
            "connection",
            "keep-alive",
            "proxy-authenticate",
            "proxy-authorization",
            "te",
            "trailer",
            "upgrade",
        )

        internal fun stripHopByHopHeaders(headers: Headers): StringValues {
            val connectionHeaders = headers[HttpHeaders.Connection]
                ?.split(",")
                ?.map { it.trim().lowercase() }
                ?.toSet()
                ?: emptySet()

            return headers.filter { key, _ ->
                key.lowercase() !in hopByHopHeaderNames && key.lowercase() !in connectionHeaders
            }
        }
    }
}
