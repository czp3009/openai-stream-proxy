package com.hiczp.openai.responses.stream.proxy

import com.hiczp.openai.responses.stream.proxy.ResponsesApiProxy.Companion.errorResponse
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
import kotlinx.io.readByteArray
import kotlinx.serialization.json.*

private val logger = KotlinLogging.logger("com.hiczp.openai.responses.stream.proxy.ResponsesApiProxy")

/**
 * Transparent proxy that converts downstream non-streaming OpenAI Responses API requests into
 * upstream SSE streaming requests, aggregates the SSE events in memory, and returns the final
 * non-streaming JSON response to the caller.
 *
 * Requests that do not match the conversion criteria (non-POST method, non-`/responses` path,
 * non-JSON content type, missing `model` field, or `stream=true`) are forwarded to the
 * upstream unchanged (passthrough).
 *
 * @param engine the [HttpClientEngine] used for outbound HTTP requests.
 *   Consumers provide a platform-specific engine (e.g. `CIO`, `OkHttp`).
 * @param upstreamBaseUrl the base URL of the upstream OpenAI-compatible server
 *   (e.g. `https://api.openai.com`). Must not end with a trailing slash.
 * @param timeoutMillis timeout in milliseconds for upstream requests.
 *   Defaults to 10 minutes (600 000 ms).
 */
class ResponsesApiProxy(
    val engine: HttpClientEngine,
    val upstreamBaseUrl: String,
    timeoutMillis: Long = 600_000L,
) {
    private val client = HttpClient(engine) {
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
     * For `POST` requests whose path ends with `/responses`, with JSON content type,
     * a `model` field in the body, and `stream` absent or `false`,
     * the request body is rewritten with `stream=true` and sent upstream as an SSE request.
     * The SSE events are consumed and aggregated into a single JSON response, which is returned
     * as an [OutgoingContent] for the caller to send downstream.
     *
     * All other requests (non-POST, non-`/responses` path, non-JSON content type,
     * missing `model` field, or already streaming) are forwarded to the upstream unchanged
     * (passthrough), and the upstream response is relayed as-is.
     *
     * ## Return value contract
     *
     * - **Non-null**: the caller should send the returned [OutgoingContent] to the downstream client
     *   (e.g. via `call.respond(result)`).
     * - **Null**: the upstream could not produce a valid response (network error, incomplete SSE stream,
     *   upstream returned a non-SSE error, etc.). The caller should either drop the downstream connection
     *   or respond with a status code that indicates an upstream error (e.g. 502 Bad Gateway).
     *
     * ## Exception contract
     *
     * This method throws an exception only for **non-network errors in the proxy itself** (e.g. bugs).
     * The caller should catch such exceptions and return an error response to the downstream client
     * (e.g. via [errorResponse]).
     *
     * @param requestMethod the HTTP method of the incoming request.
     * @param requestUri the request URI (path and query string) relative to the proxy root.
     * @param requestHeaders the incoming request headers.
     * @param requestBody the incoming request body.
     * @return an [OutgoingContent] to send downstream, or `null` if the upstream failed.
     * @throws Exception for non-network errors in the proxy logic that the caller should translate
     *   into an error response for the downstream client.
     */
    suspend fun proxy(
        requestMethod: HttpMethod,
        requestUri: String,
        requestHeaders: Headers,
        requestBody: ByteReadChannel,
    ): OutgoingContent? {
        val path = requestUri.substringBefore('?').trimEnd('/')
        val upstreamUrl = upstreamBaseUrl.trimEnd('/') + requestUri

        if (requestMethod != HttpMethod.Post || !path.endsWith("/responses")) {
            logger.debug { "Passthrough: ${requestMethod.value} $requestUri (not OpenAI Responses request)" }
            return passthrough(upstreamUrl, requestMethod, requestHeaders, requestBody)
        }

        val contentType = requestHeaders[HttpHeaders.ContentType]
            ?.let { runCatching { ContentType.parse(it) }.getOrNull() }
        if (contentType?.match(ContentType.Application.Json) != true) {
            logger.debug { "Passthrough: ${requestMethod.value} $requestUri (non-JSON)" }
            return passthrough(upstreamUrl, requestMethod, requestHeaders, requestBody)
        }

        val bodyBytes = try {
            requestBody.readRemaining().readByteArray()
        } catch (e: IOException) {
            //downstream error
            logger.warn { "Failed to read request body from downstream: ${e.message}" }
            throw e
        }
        val bodyJson = runCatching { Json.parseToJsonElement(bodyBytes.decodeToString()) }.getOrNull()

        if (bodyJson !is JsonObject || !bodyJson.contains("model")) {
            logger.debug { "Passthrough: ${requestMethod.value} $requestUri (not OpenAI Responses request)" }
            return passthrough(upstreamUrl, requestMethod, requestHeaders, bodyBytes)
        }

        if (bodyJson["stream"]?.jsonPrimitive?.booleanOrNull == true) {
            logger.debug { "Passthrough: ${requestMethod.value} $requestUri (already streaming)" }
            return passthrough(upstreamUrl, requestMethod, requestHeaders, bodyBytes)
        }

        logger.info { "Proxy flow: ${requestMethod.value} $requestUri" }
        return convert(upstreamUrl, requestHeaders, bodyJson)
    }

    suspend fun convert(
        upstreamUrl: String,
        headers: Headers,
        bodyJson: JsonObject,
    ): OutgoingContent? {
        val rewrittenBody = JsonObject(bodyJson + ("stream" to JsonPrimitive(true)))
        val forwardHeaders = stripHopByHopHeaders(headers).filter { key, _ ->
            !key.equals(HttpHeaders.ContentType, ignoreCase = true)
        }

        val accumulator = ResponseAccumulator()
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
                incoming.collect { event -> accumulator.accumulate(event) }
            }
        } catch (e: SSEClientException) {
            val response = e.response
            return if (e.cause == null) {
                //SSE stream isn't started
                if (response != null) {
                    //the server responds, but not SSE or status code not 200
                    logger.info { "Server returned non-SSE response with status ${response.status}" }
                    val filteredHeaders = response.headers.filter { key, _ ->
                        !key.equals(HttpHeaders.TransferEncoding, ignoreCase = true)
                    }
                    val bodyChannel = response.bodyAsChannel()
                    object : OutgoingContent.ReadChannelContent() {
                        override val contentLength = response.contentLength()
                        override val status = response.status
                        override val headers = Headers.build { appendAll(filteredHeaders) }
                        override fun readFrom() = bodyChannel
                    }
                } else {
                    //impossible
                    logger.warn { "Unexpected SSEClientException for $upstreamUrl: ${e.message}" }
                    null
                }
            } else {
                if (e.cause is SSEClientException) {
                    //network error, SSE parsing failure, or reconnection exhaustion
                    logger.warn { "SSE stream failed for $upstreamUrl: ${e.cause?.message}" }
                    null
                } else {
                    //non-network error (e.g., bug in accumulator)
                    //caller should convert the exception to errorResponse
                    logger.warn { "Unexpected error for $upstreamUrl: ${e.cause?.message}" }
                    throw e
                }
            }
        } catch (e: IOException) {
            //other network error
            logger.warn { "SSE request IOException for $upstreamUrl: ${e.message}" }
            return null
        }

        if (!accumulator.isTerminated) {
            logger.warn { "SSE stream ended without terminal event for $upstreamUrl" }
            return null
        }

        val response = accumulator.response
        val terminalType = accumulator.terminalType ?: return null
        val statusCode = resolveStatusCode(terminalType, response)

        val responseHeaders = (sessionHeaders ?: return null).filter { key, _ ->
            !key.equals(HttpHeaders.TransferEncoding, ignoreCase = true) &&
                    !key.equals(HttpHeaders.ContentType, ignoreCase = true)
        }.let {
            Headers.build {
                appendAll(it)
                append(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            }
        }

        val responseBytes = response.toString().encodeToByteArray()
        logger.info { "Completed: $upstreamUrl → $statusCode ($terminalType)" }
        return object : OutgoingContent.ByteArrayContent() {
            override val contentLength = responseBytes.size.toLong()
            override val status = statusCode
            override val headers = responseHeaders
            override fun bytes() = responseBytes
        }
    }

    suspend fun passthrough(
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

    suspend fun passthrough(
        upstreamUrl: String,
        method: HttpMethod,
        headers: Headers,
        bodyBytes: ByteArray,
    ): OutgoingContent? = passthrough(upstreamUrl, method, headers, ByteReadChannel(bodyBytes))

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

    private fun stripHopByHopHeaders(headers: Headers): StringValues {
        val connectionHeaders = headers[HttpHeaders.Connection]
            ?.split(",")
            ?.map { it.trim().lowercase() }
            ?.toSet()
            ?: emptySet()

        return headers.filter { key, _ ->
            key.lowercase() !in hopByHopHeaderNames && key.lowercase() !in connectionHeaders
        }
    }

    private fun resolveStatusCode(
        terminalType: ResponseAccumulator.TerminalType,
        response: JsonObject,
    ): HttpStatusCode = when (terminalType) {
        ResponseAccumulator.TerminalType.COMPLETED -> HttpStatusCode.OK
        ResponseAccumulator.TerminalType.FAILED -> mapFailedStatusCode(response)
        ResponseAccumulator.TerminalType.INCOMPLETE -> HttpStatusCode.BadGateway
    }

    private fun mapFailedStatusCode(response: JsonObject): HttpStatusCode {
        val errorCode = response["error"]?.jsonObject?.get("code")?.jsonPrimitive?.content
        return when (errorCode) {
            "insufficient_quota" -> HttpStatusCode.TooManyRequests
            "rate_limit_exceeded" -> HttpStatusCode.TooManyRequests
            "usage_not_included" -> HttpStatusCode.TooManyRequests
            "server_is_overloaded" -> HttpStatusCode.ServiceUnavailable
            "slow_down" -> HttpStatusCode.ServiceUnavailable
            "server_error" -> HttpStatusCode.InternalServerError
            else -> HttpStatusCode.BadRequest
        }
    }

    companion object {
        /**
         * Builds an OpenAI-compatible error response body.
         *
         * For example, callers can use this when [proxy] returns `null` (e.g. HTTP 502 Bad Gateway)
         * or throws an exception (e.g. HTTP 500 Internal Server Error).
         *
         * @param message the human-readable error message (placed in `error.message`).
         * @param type the error type (placed in `error.type`).
         * @param param the error param (placed in `error.param`). Defaults to `""`.
         * @param code the error code (placed in `error.code`). Defaults to [type].
         * @param status the HTTP status code for the response. Defaults to `502 Bad Gateway`.
         * @return a [ByteArrayContent] with content type `application/json`
         *   and an error object following the OpenAI error envelope format.
         */
        fun errorResponse(
            message: String,
            type: String,
            param: String = "",
            code: String = type,
            status: HttpStatusCode = HttpStatusCode.BadGateway,
        ): ByteArrayContent {
            val body = JsonObject(
                mapOf(
                    "error" to JsonObject(
                        mapOf(
                            "message" to JsonPrimitive(message),
                            "type" to JsonPrimitive(type),
                            "param" to JsonPrimitive(param),
                            "code" to JsonPrimitive(code),
                        )
                    )
                )
            )
            return ByteArrayContent(
                bytes = body.toString().encodeToByteArray(),
                contentType = ContentType.Application.Json,
                status = status,
            )
        }
    }
}
