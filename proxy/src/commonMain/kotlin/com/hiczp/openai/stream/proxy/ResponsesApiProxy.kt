package com.hiczp.openai.stream.proxy

import com.hiczp.openai.stream.proxy.ResponsesApiProxy.Companion.errorResponse
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

private val logger = KotlinLogging.logger("com.hiczp.openai.stream.proxy.ResponsesApiProxy")

/**
 * Transparent proxy that converts downstream non-streaming OpenAI Responses API requests into
 * upstream SSE streaming requests, aggregates the SSE events in memory, and returns a downstream
 * response that behaves like the non-streaming Responses API.
 *
 * Terminal `response.completed` and `response.incomplete` events are returned as normal
 * non-streaming Response objects with HTTP `200 OK`. Terminal `response.failed` events that
 * contain an upstream error object are converted into OpenAI-compatible non-streaming error
 * responses, with the HTTP status derived from `error.code` when it is present. A terminal
 * failed Response without an error object is returned as a Response object with HTTP `200 OK`;
 * an error object without `error.code` still produces an error body but leaves the HTTP status
 * as `200 OK`.
 * Upstream responses that fail before the SSE stream starts are relayed as-is. Network failures,
 * SSE client failures, and SSE streams without a terminal event return `null` so the caller can
 * choose the downstream error response.
 *
 * Requests that do not match the conversion criteria (non-POST method, non-`/responses` path,
 * non-JSON content type, missing `model` field, or `stream=true`) are forwarded to the
 * upstream unchanged (passthrough).
 *
 * @param engine the [HttpClientEngine] used for outbound HTTP requests.
 *   Consumers provide a platform-specific engine (e.g. `CIO`, `OkHttp`).
 * @param upstreamBaseUrl the base URL of the upstream OpenAI-compatible server
 *   (e.g. `https://api.openai.com`). A trailing slash is ignored.
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
     * The SSE events are consumed and aggregated into a terminal Response object. Terminal
     * `completed` and `incomplete` Response objects are returned with HTTP `200 OK`. Terminal
     * `failed` Response objects with an upstream error object are converted into
     * OpenAI-compatible error bodies, and `error.code` is used to choose non-2xx statuses that
     * preserve SDK retry/error handling. Terminal failed Responses without an error object are
     * returned as Response objects with HTTP `200 OK`; an error object without `error.code`
     * still produces an error body but keeps HTTP `200 OK`.
     * Upstream responses that fail before an SSE stream starts are relayed as-is.
     *
     * All other requests (non-POST, non-`/responses` path, non-JSON content type,
     * missing `model` field, or already streaming) are forwarded to the upstream unchanged
     * (passthrough), and the upstream response is relayed as-is.
     *
     * ## Return value contract
     *
     * - **Non-null**: the caller should send the returned [OutgoingContent] to the downstream client
     *   (e.g. via `call.respond(result)`).
     * - **Null**: the upstream did not produce a relayable response or stream terminal result
     *   (network error, SSE client failure, SSE stream without a terminal event, etc.). The caller
     *   should either drop the downstream connection or respond with a status code that indicates
     *   an upstream error (e.g. 502 Bad Gateway).
     *
     * ## Exception contract
     *
     * This method throws an exception only for unexpected non-network failures while processing the
     * proxy flow (e.g. malformed upstream event data or bugs). The caller should catch such
     * exceptions and return an error response to the downstream client (e.g. via [errorResponse]).
     *
     * @param requestMethod the HTTP method of the incoming request.
     * @param requestUri the request URI (path and query string) relative to the proxy root.
     * @param requestHeaders the incoming request headers.
     * @param requestBody the incoming request body.
     * @return an [OutgoingContent] to send downstream, or `null` if no relayable upstream response
     *   and no terminal stream result could be produced.
     * @throws Exception for unexpected non-network failures that the caller should translate into
     *   an error response for the downstream client.
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
        val failedError = if (terminalType == ResponseAccumulator.TerminalType.FAILED) {
            response["error"]?.jsonObject
        } else {
            null
        }
        val failedErrorCode = failedError?.get("code")?.jsonPrimitive?.contentOrNull
        val statusCode = resolveStatusCode(terminalType, failedErrorCode)
        val responseBytes = if (failedError != null) {
            val type = failedError["type"]?.jsonPrimitive?.contentOrNull
                ?: failedErrorCode?.let(::mapFailedErrorType)
                ?: "upstream_error"
            errorResponseBody(
                message = failedError["message"]?.jsonPrimitive?.contentOrNull ?: "Upstream response failed",
                type = type,
                param = failedError["param"]?.jsonPrimitive?.contentOrNull ?: "",
                code = failedErrorCode ?: type,
            )
        } else {
            response.toString().encodeToByteArray()
        }

        val responseHeaders = (sessionHeaders ?: return null).filter { key, _ ->
            !key.equals(HttpHeaders.TransferEncoding, ignoreCase = true) &&
                    !key.equals(HttpHeaders.ContentType, ignoreCase = true)
        }.let {
            Headers.build {
                appendAll(it)
                append(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            }
        }

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
        failedErrorCode: String?,
    ): HttpStatusCode = when (terminalType) {
        ResponseAccumulator.TerminalType.COMPLETED -> HttpStatusCode.OK
        ResponseAccumulator.TerminalType.FAILED -> failedErrorCode?.let(::mapFailedStatusCode) ?: HttpStatusCode.OK
        ResponseAccumulator.TerminalType.INCOMPLETE -> HttpStatusCode.OK
    }

    private fun mapFailedStatusCode(errorCode: String): HttpStatusCode =
        when (errorCode) {
            "insufficient_quota",
            "rate_limit_exceeded",
            "usage_not_included" -> HttpStatusCode.TooManyRequests

            "server_is_overloaded",
            "slow_down" -> HttpStatusCode.ServiceUnavailable

            "server_error" -> HttpStatusCode.InternalServerError
            "vector_store_timeout" -> HttpStatusCode.GatewayTimeout
            in invalidRequestErrorCodes -> HttpStatusCode.BadRequest
            else -> HttpStatusCode.BadRequest
        }

    private fun mapFailedErrorType(errorCode: String): String =
        when (errorCode) {
            "insufficient_quota",
            "rate_limit_exceeded",
            "usage_not_included" -> "rate_limit_error"

            "server_is_overloaded",
            "slow_down",
            "server_error",
            "vector_store_timeout" -> "server_error"

            in invalidRequestErrorCodes -> "invalid_request_error"
            else -> "invalid_request_error"
        }

    private val invalidRequestErrorCodes = setOf(
        "invalid_prompt",
        "invalid_image",
        "invalid_image_format",
        "invalid_base64_image",
        "invalid_image_url",
        "image_too_large",
        "image_too_small",
        "image_parse_error",
        "image_content_policy_violation",
        "invalid_image_mode",
        "image_file_too_large",
        "unsupported_image_media_type",
        "empty_image_file",
        "failed_to_download_image",
        "image_file_not_found",
    )

    companion object {
        /**
         * Builds the JSON body bytes for an OpenAI-compatible error envelope.
         *
         * @param message the human-readable error message (placed in `error.message`).
         * @param type the error type (placed in `error.type`).
         * @param param the error param (placed in `error.param`). Defaults to `""`.
         * @param code the error code (placed in `error.code`). Defaults to [type].
         * @return a JSON byte array with an OpenAI error envelope.
         */
        fun errorResponseBody(
            message: String,
            type: String,
            param: String = "",
            code: String = type,
        ): ByteArray {
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
            return body.toString().encodeToByteArray()
        }

        /**
         * Builds an OpenAI-compatible error response.
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
            return ByteArrayContent(
                bytes = errorResponseBody(
                    message = message,
                    type = type,
                    param = param,
                    code = code,
                ),
                contentType = ContentType.Application.Json,
                status = status,
            )
        }
    }
}
