package com.hiczp.openai.stream.proxy

import com.hiczp.openai.stream.proxy.AbstractApiProxy.Companion.stripHopByHopHeaders
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

/**
 * Shared base class for OpenAI API proxies.
 *
 * Provides the upstream HTTP client (with SSE and timeout plugins), [passthrough] for forwarding
 * requests unchanged, and [stripHopByHopHeaders] (internal companion utility). Subclasses
 * implement [proxy] to define protocol-specific conversion logic.
 *
 * @param engine the HTTP client engine used for upstream requests
 * @param upstreamBaseUrl the base URL of the upstream OpenAI-compatible server (e.g. `https://api.openai.com`)
 * @param timeoutMillis timeout in milliseconds for request, connect, and socket operations (default 10 minutes)
 */
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

    /**
     * Proxies a single downstream request to the upstream server.
     *
     * Subclasses implement this method to define protocol-specific conversion logic (e.g. converting
     * a non-streaming request to an upstream SSE stream and aggregating the result).
     *
     * ## Return value contract
     *
     * - **Non-null**: the caller should send the returned [OutgoingContent] to the downstream client
     *   (e.g. via `call.respond(result)`).
     * - **Null**: the upstream did not produce a reliable response (network error, upstream failure,
     *   etc.). The caller should either drop the downstream connection or respond with a status code
     *   that indicates an upstream error (e.g. 502 Bad Gateway).
     *
     * ## Exception contract
     *
     * This method may throw exceptions for unexpected non-network failures while processing the proxy
     * flow (e.g. malformed upstream data or bugs). The caller should catch such exceptions and return
     * an error response to the downstream client (e.g. via [OpenAiErrors.errorResponse]).
     *
     * @param requestMethod the HTTP method of the downstream request
     * @param requestUri the request URI (path and query string) of the downstream request
     * @param requestHeaders the headers of the downstream request
     * @param requestBody the body of the downstream request
     */
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
