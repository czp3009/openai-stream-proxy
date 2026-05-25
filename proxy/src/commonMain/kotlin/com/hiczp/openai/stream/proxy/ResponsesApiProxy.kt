package com.hiczp.openai.stream.proxy

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.engine.*
import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.util.*
import kotlinx.serialization.json.*

/**
 * Transparent proxy that converts downstream non-streaming OpenAI Responses API requests into
 * upstream SSE streaming requests, aggregates the SSE events in memory, and sends a downstream
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
 * SSE client failures, and SSE streams without a terminal event are handled by [AbstractApiProxy]
 * as upstream errors.
 *
 * Requests that do not match the conversion criteria (non-POST method, non-`/responses` path,
 * non-JSON content type, missing `model` field, or `stream=true`) are forwarded to the
 * upstream unchanged (passthrough).
 */
class ResponsesApiProxy(
    engine: HttpClientEngine,
    upstreamBaseUrl: String,
    timeoutMillis: Long = 600_000L,
) : AbstractApiProxy(engine, upstreamBaseUrl, timeoutMillis) {
    override val logger = KotlinLogging.logger("com.hiczp.openai.stream.proxy.ResponsesApiProxy")

    override fun needConvert(method: HttpMethod, path: String): Boolean =
        method == HttpMethod.Post && path.endsWith("/responses")

    override fun rewriteBody(bodyJson: JsonObject): JsonObject =
        JsonObject(bodyJson + ("stream" to JsonPrimitive(true)))

    override fun createAccumulator(): SseAccumulator = ResponseAccumulator()

    @Suppress("DuplicatedCode")
    override fun buildResult(accumulator: SseAccumulator, sessionHeaders: Headers): OutgoingContent {
        val responseAccumulator = accumulator as ResponseAccumulator
        val response = responseAccumulator.response
        val terminalType =
            responseAccumulator.terminalType ?: error("Response accumulator terminated without terminal type")
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
            OpenAiErrors.errorResponseBody(
                message = failedError["message"]?.jsonPrimitive?.contentOrNull ?: "Upstream response failed",
                type = type,
                param = failedError["param"]?.jsonPrimitive?.contentOrNull ?: "",
                code = failedErrorCode ?: type,
            )
        } else {
            response.toString().encodeToByteArray()
        }

        val responseHeaders = sessionHeaders.filter { key, _ ->
            !key.equals(HttpHeaders.TransferEncoding, ignoreCase = true) &&
                    !key.equals(HttpHeaders.ContentType, ignoreCase = true)
        }.let {
            Headers.build {
                appendAll(it)
                append(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            }
        }

        logger.debug { "Convert completed: status=${statusCode.value} terminal=$terminalType bytes=${responseBytes.size}" }
        return object : OutgoingContent.ByteArrayContent() {
            override val contentLength = responseBytes.size.toLong()
            override val status = statusCode
            override val headers = responseHeaders
            override fun bytes() = responseBytes
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
}
