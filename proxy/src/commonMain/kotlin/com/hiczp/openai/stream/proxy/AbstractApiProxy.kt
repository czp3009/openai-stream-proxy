package com.hiczp.openai.stream.proxy

import com.hiczp.openai.stream.proxy.AbstractApiProxy.Companion.stripHopByHopHeaders
import io.github.oshai.kotlinlogging.KLogger
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
import kotlinx.coroutines.flow.any
import kotlinx.io.IOException
import kotlinx.io.readByteArray
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.jsonPrimitive

/**
 * Shared base class for OpenAI API proxies using the template method pattern.
 *
 * Provides the upstream HTTP client (with SSE and timeout plugins), [passthrough] for forwarding
 * requests unchanged, and [stripHopByHopHeaders]. The public entry point [proxy] is a template
 * method that validates the request and delegates to [convert] (another template method) for
 * SSE-specific processing. Subclasses implement [needConvert], [rewriteBody], [createAccumulator],
 * and [buildResult] to define protocol-specific conversion logic.
 *
 * ## Resource lifecycle
 *
 * The [HttpClientEngine] provided via [engine] is **not** owned by this class. The caller is
 * responsible for closing the engine when it is no longer needed (e.g. on application shutdown).
 * Do **not** call [HttpClient.close] on the internal client — doing so would shut down the shared
 * engine and break any other clients using it.
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
    protected abstract val logger: KLogger

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
     * This is a template method that checks whether the request should be converted using
     * [needConvert], validates the request (JSON content type, `model` field, not already streaming),
     * and delegates to [convert] for protocol-specific processing. Requests that do not match the
     * conversion criteria are forwarded unchanged via [passthrough]. Responses are sent through
     * [respond] inside this method so streaming passthrough responses can be consumed while the
     * upstream response lifecycle is still open.
     *
     * ## Exception contract
     *
     * This method may throw exceptions for unexpected non-network failures while processing the proxy
     * flow (e.g. accumulator or result assembly bugs). The caller should catch such exceptions and
     * return an error response to the downstream client (e.g. via [OpenAiErrors.errorResponse]).
     *
     * @param requestMethod the HTTP method of the downstream request
     * @param requestUri the request URI (path and query string) of the downstream request
     * @param requestHeaders the headers of the downstream request
     * @param requestBody the body of the downstream request
     * @param respond sends a prepared downstream response
     */
    suspend fun proxy(
        requestMethod: HttpMethod,
        requestUri: String,
        requestHeaders: Headers,
        requestBody: ByteReadChannel,
        respond: suspend (OutgoingContent) -> Unit,
    ) {
        val path = requestUri.substringBefore('?').trimEnd('/')
        val upstreamUrl = upstreamBaseUrl.trimEnd('/') + requestUri

        if (!needConvert(requestMethod, path)) {
            logger.debug { "Passthrough: ${requestMethod.value} $requestUri (path not match)" }
            passthrough(upstreamUrl, requestMethod, requestHeaders, requestBody, respond)
            return
        }

        val contentType = requestHeaders[HttpHeaders.ContentType]
            ?.let { runCatching { ContentType.parse(it) }.getOrNull() }
        if (contentType?.match(ContentType.Application.Json) != true) {
            logger.debug { "Passthrough: ${requestMethod.value} $requestUri (non-JSON request)" }
            passthrough(upstreamUrl, requestMethod, requestHeaders, requestBody, respond)
            return
        }

        val bodyBytes = try {
            requestBody.readRemaining().readByteArray()
        } catch (e: IOException) {
            logger.warn { "Failed to read request body from downstream: ${e.message}" }
            throw e
        }
        val bodyJson = runCatching { Json.parseToJsonElement(bodyBytes.decodeToString()) }.getOrNull()

        if (bodyJson !is JsonObject || !bodyJson.contains("model")) {
            logger.debug { "Passthrough: ${requestMethod.value} $requestUri (not a valid API request)" }
            passthrough(upstreamUrl, requestMethod, requestHeaders, bodyBytes, respond)
            return
        }

        if (bodyJson["stream"]?.jsonPrimitive?.booleanOrNull == true) {
            logger.debug { "Passthrough: ${requestMethod.value} $requestUri (already streaming)" }
            passthrough(upstreamUrl, requestMethod, requestHeaders, bodyBytes, respond)
            return
        }

        logger.debug { "Proxy flow: ${requestMethod.value} $requestUri" }
        convert(upstreamUrl, requestHeaders, bodyJson, respond)
    }

    /**
     * Determines whether the given request should be converted (rewritten to an upstream SSE stream
     * and aggregated) based on its HTTP method and path.
     *
     * Subclasses implement this to define which paths they handle (e.g. `/responses`,
     * `/chat/completions`). Requests that do not match are forwarded unchanged via [passthrough].
     *
     * @param method the HTTP method of the downstream request
     * @param path the path component of the request URI (without query string, trailing slash trimmed)
     */
    protected abstract fun needConvert(method: HttpMethod, path: String): Boolean

    /**
     * Converts a non-streaming request to an upstream SSE stream, aggregates the result, and
     * sends the downstream response.
     *
     * This is a template method that rewrites the request body via [rewriteBody], sends it upstream
     * as an SSE request, collects events into an accumulator created by [createAccumulator], and
     * delegates result assembly to [buildResult]. SSE error handling (non-SSE upstream response
     * relay, network failure, incomplete stream) is handled here.
     *
     * @param upstreamUrl the full upstream URL to send the rewritten request to
     * @param headers the original downstream request headers
     * @param bodyJson the parsed JSON body of the downstream request
     * @param respond sends a prepared downstream response
     */
    protected suspend fun convert(
        upstreamUrl: String,
        headers: Headers,
        bodyJson: JsonObject,
        respond: suspend (OutgoingContent) -> Unit,
    ) {
        val rewrittenBody = rewriteBody(bodyJson)
        val forwardHeaders = stripHopByHopHeaders(headers).filter { key, _ ->
            !key.equals(HttpHeaders.ContentType, ignoreCase = true)
        }

        val accumulator = createAccumulator()
        var sessionHeaders: Headers? = null

        try {
            client.sse({
                this.url(upstreamUrl)
                this.method = HttpMethod.Post
                this.headers.appendAll(forwardHeaders)
                this.contentType(ContentType.Application.Json)
                this.setBody(rewrittenBody.toString())
            }) {
                sessionHeaders = call.response.headers
                incoming.any { event ->
                    accumulator.accumulate(event)
                    accumulator.isTerminated
                }
            }
        } catch (e: SSEClientException) {
            val response = e.response
            val cause = e.cause
            if (cause == null) {
                //SSE stream isn't started
                if (response != null) {
                    //the server responds, but not SSE or status code not 200
                    logger.warn { "Server returned non-SSE response with status ${response.status}" }
                    respond(response.toOutgoingContent())
                    return
                } else {
                    //impossible
                    logger.warn { "Unexpected SSEClientException for $upstreamUrl: ${e.message}" }
                    respondUpstreamError(respond)
                    return
                }
            } else {
                if (cause is SSEClientException || (response == null && cause is IOException)) {
                    //network error before the SSE session starts, SSE parsing failure, or reconnection exhaustion
                    logger.warn { "SSE stream failed for $upstreamUrl: ${cause.message ?: e.message}" }
                    respondUpstreamError(respond)
                    return
                } else {
                    //non-network error (e.g., bug in accumulator)
                    //caller should convert the exception to errorResponse
                    logger.warn { "Unexpected error for $upstreamUrl: ${cause.message ?: e.message}" }
                    throw e
                }
            }
        } catch (e: IOException) {
            //other network error
            logger.warn { "SSE request IOException for $upstreamUrl: ${e.message}" }
            respondUpstreamError(respond)
            return
        }

        if (!accumulator.isTerminated) {
            logger.warn { "SSE stream ended without terminal event for $upstreamUrl" }
            respondUpstreamError(respond)
            return
        }

        respond(buildResult(accumulator, sessionHeaders ?: error("SSE session completed without response headers")))
    }

    /**
     * Rewrites the downstream request body to enable SSE streaming on the upstream request.
     *
     * Subclasses implement this to add protocol-specific fields (e.g. `stream=true` for the
     * Responses API, `stream=true` + `stream_options` for Chat Completions).
     *
     * @param bodyJson the original downstream request body
     * @return the rewritten body to send upstream
     */
    protected abstract fun rewriteBody(bodyJson: JsonObject): JsonObject

    /**
     * Creates the SSE event accumulator for this protocol.
     *
     * Subclasses implement this to return the appropriate accumulator (e.g. [ResponseAccumulator],
     * [ChatCompletionsAccumulator]).
     */
    protected abstract fun createAccumulator(): SseAccumulator

    /**
     * Assembles the final downstream response from the accumulated SSE result.
     *
     * Called only after the SSE stream has been consumed successfully and the accumulator reports
     * [SseAccumulator.isTerminated] as `true`. Subclasses implement this to define protocol-specific
     * result assembly (e.g. status code mapping, error body formatting).
     *
     * @param accumulator the accumulator populated with upstream SSE events
     * @param sessionHeaders the headers received from the upstream SSE response
     * @return the downstream response content
     */
    protected abstract fun buildResult(
        accumulator: SseAccumulator,
        sessionHeaders: Headers,
    ): OutgoingContent

    protected suspend fun passthrough(
        upstreamUrl: String,
        method: HttpMethod,
        headers: Headers,
        body: ByteReadChannel,
        respond: suspend (OutgoingContent) -> Unit,
    ) {
        val forwardHeaders = stripHopByHopHeaders(headers)
        var upstreamResponseReceived = false

        try {
            client.prepareRequest(upstreamUrl) {
                this.method = method
                this.headers.appendAll(forwardHeaders)
                this.setBody(body)
            }.execute { upstreamResponse ->
                upstreamResponseReceived = true
                val responseContent = upstreamResponse.toOutgoingContent()
                respond(responseContent)
            }
        } catch (e: IOException) {
            logger.warn { "Passthrough request to $upstreamUrl failed: ${e.message}" }
            if (!upstreamResponseReceived) {
                respondUpstreamError(respond)
            }
        }
    }

    protected suspend fun passthrough(
        upstreamUrl: String,
        method: HttpMethod,
        headers: Headers,
        bodyBytes: ByteArray,
        respond: suspend (OutgoingContent) -> Unit,
    ) = passthrough(upstreamUrl, method, headers, ByteReadChannel(bodyBytes), respond)

    private suspend fun respondUpstreamError(respond: suspend (OutgoingContent) -> Unit) {
        respond(
            OpenAiErrors.errorResponse(
                message = "Upstream returned incomplete or invalid response",
                type = "upstream_error",
            )
        )
    }

    private fun HttpResponse.toOutgoingContent(): OutgoingContent {
        val response = this
        val filteredHeaders = stripHopByHopHeaders(response.headers).filter { key, _ ->
            !key.equals(HttpHeaders.ContentType, ignoreCase = true)
        }
        return object : OutgoingContent.WriteChannelContent() {
            override val contentLength = response.contentLength()
            override val contentType = response.contentType()
            override val status = response.status
            override val headers = Headers.build { appendAll(filteredHeaders) }
            override suspend fun writeTo(channel: ByteWriteChannel) {
                response.bodyAsChannel().copyTo(channel)
            }
        }
    }

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
