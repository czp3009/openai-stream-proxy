package com.hiczp.openai.responses.stream.proxy

import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.client.plugins.sse.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.util.*
import io.ktor.utils.io.*
import kotlinx.io.readByteArray
import kotlinx.serialization.json.*

class ResponsesApiProxy(
    val engine: HttpClientEngine,
    val upstreamBaseUrl: String,
) {
    private val client = HttpClient(engine) {
        install(SSE)
    }

    suspend fun proxy(
        requestMethod: HttpMethod,
        requestPath: String,
        requestHeaders: Headers,
        requestBody: ByteReadChannel,
    ): OutgoingContent? {
        val upstreamUrl = upstreamBaseUrl.trimEnd('/') + requestPath

        val contentType = requestHeaders[HttpHeaders.ContentType]
            ?.let { runCatching { ContentType.parse(it) }.getOrNull() }
        if (contentType?.match(ContentType.Application.Json) != true) {
            return passthrough(upstreamUrl, requestMethod, requestHeaders, requestBody)
        }

        val bodyBytes = requestBody.readRemaining().readByteArray()
        val bodyJson = runCatching { Json.parseToJsonElement(bodyBytes.decodeToString()) }.getOrNull()

        if (bodyJson is JsonObject && bodyJson["stream"]?.jsonPrimitive?.booleanOrNull != true) {
            return proxyFlow(upstreamUrl, requestHeaders, bodyJson)
        }

        return passthrough(upstreamUrl, requestMethod, requestHeaders, bodyBytes)
    }

    private suspend fun proxyFlow(
        upstreamUrl: String,
        headers: Headers,
        bodyJson: JsonObject,
    ): OutgoingContent? {
        val rewrittenBody = JsonObject(bodyJson + ("stream" to JsonPrimitive(true)))
        val forwardHeaders = headers.filter { key, _ ->
            !key.equals(HttpHeaders.Host, ignoreCase = true)
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
            return if (e.cause == null && response != null) {
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
                throw e
            }
        }

        if (!accumulator.isTerminated) return null

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
        return object : OutgoingContent.ByteArrayContent() {
            override val contentLength = responseBytes.size.toLong()
            override val status = statusCode
            override val headers = responseHeaders
            override fun bytes() = responseBytes
        }
    }

    private suspend fun passthrough(
        upstreamUrl: String,
        method: HttpMethod,
        headers: Headers,
        body: ByteReadChannel,
    ): OutgoingContent {
        val forwardHeaders = headers.filter { key, _ ->
            !key.equals(HttpHeaders.Host, ignoreCase = true)
        }

        val upstreamResponse = client.request(upstreamUrl) {
            this.method = method
            this.headers.appendAll(forwardHeaders)
            this.setBody(body)
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

    private suspend fun passthrough(
        upstreamUrl: String,
        method: HttpMethod,
        headers: Headers,
        bodyBytes: ByteArray,
    ): OutgoingContent = passthrough(upstreamUrl, method, headers, ByteReadChannel(bodyBytes))

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
}
