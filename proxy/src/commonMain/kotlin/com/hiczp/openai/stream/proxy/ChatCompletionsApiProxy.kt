package com.hiczp.openai.stream.proxy

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.engine.*
import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.util.*
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Transparent proxy that converts downstream non-streaming OpenAI Chat Completions API requests into
 * upstream SSE streaming requests, aggregates the SSE events in memory, and sends a downstream
 * response that behaves like the non-streaming Chat Completions API.
 *
 * The upstream SSE is consumed by [ChatCompletionsAccumulator], which merges streamed chunk deltas
 * into a single `chat.completion` JSON object. The terminal marker is `data: [DONE]`.
 *
 * Upstream responses that fail before the SSE stream starts are relayed as-is. Network failures,
 * SSE client failures, and SSE streams without a terminal `[DONE]` event are handled by
 * [AbstractApiProxy] as upstream errors.
 *
 * Requests that do not match the conversion criteria (non-POST method, non-`/chat/completions` path,
 * non-JSON content type, missing `model` field, or `stream=true`) are forwarded to the
 * upstream unchanged (passthrough).
 */
class ChatCompletionsApiProxy(
    engine: HttpClientEngine,
    upstreamBaseUrl: String,
    timeoutMillis: Long = 600_000L,
) : AbstractApiProxy(engine, upstreamBaseUrl, timeoutMillis) {
    override val logger = KotlinLogging.logger("com.hiczp.openai.stream.proxy.ChatCompletionsApiProxy")

    override fun needConvert(method: HttpMethod, path: String): Boolean =
        method == HttpMethod.Post && path.endsWith("/chat/completions")

    override fun rewriteBody(bodyJson: JsonObject): JsonObject =
        JsonObject(buildMap {
            putAll(bodyJson)
            put("stream", JsonPrimitive(true))
            put("stream_options", JsonObject(mapOf("include_usage" to JsonPrimitive(true))))
        })

    override fun createAccumulator(): SseAccumulator = ChatCompletionsAccumulator()

    @Suppress("DuplicatedCode")
    override fun buildResult(accumulator: SseAccumulator, sessionHeaders: Headers): OutgoingContent {
        val chatAccumulator = accumulator as ChatCompletionsAccumulator
        val response = chatAccumulator.response
        val responseBytes = response.toString().encodeToByteArray()

        val responseHeaders = Headers.build {
            appendAll(stripHopByHopHeaders(sessionHeaders).filter { key, _ ->
                !key.equals(HttpHeaders.ContentType, ignoreCase = true) &&
                        !key.equals(HttpHeaders.ContentEncoding, ignoreCase = true)
            })
        }

        logger.debug { "Convert completed: status=${HttpStatusCode.OK.value} terminal=DONE bytes=${responseBytes.size}" }
        return object : OutgoingContent.ByteArrayContent() {
            override val contentLength = responseBytes.size.toLong()
            override val contentType = ContentType.Application.Json
            override val status = HttpStatusCode.OK
            override val headers = responseHeaders
            override fun bytes() = responseBytes
        }
    }
}
