package com.hiczp.openai.responses.stream.proxy.sniffer

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.utils.io.*
import io.ktor.util.*

private val logger = KotlinLogging.logger {}

// Streaming reverse proxy: forwards requests to an HTTPS upstream
// without buffering bodies in memory.
class ReverseProxy(
    private val client: HttpClient,
    private val upstreamUrl: String,
) {
    @OptIn(InternalAPI::class)
    suspend fun forward(call: ApplicationCall) {
        val request = call.request
        val targetUrl = upstreamUrl.trimEnd('/') + request.uri

        TrafficLogger.logRequest(request.httpMethod, request.uri, request.headers)

        // Tee the request body: log while forwarding to upstream
        val requestBody = streamRequestBody(request.headers, call.request.receiveChannel())

        val upstreamResponse = client.request(targetUrl) {
            method = request.httpMethod
            headers {
                request.headers.forEach { name, values ->
                    if (name.equals(HttpHeaders.Host, ignoreCase = true)) return@forEach
                    values.forEach { append(name, it) }
                }
            }
            body = requestBody
        }

        TrafficLogger.logResponse(upstreamResponse.status, upstreamResponse.headers)

        respondWithBody(call, upstreamResponse)
    }

    // Wrap the incoming channel as OutgoingContent, logging each chunk before forwarding.
    private fun streamRequestBody(
        requestHeaders: Headers,
        incoming: ByteReadChannel,
    ): OutgoingContent.WriteChannelContent {
        val filteredHeaders = Headers.build {
            requestHeaders.forEach { name, values ->
                if (name.equals(HttpHeaders.Host, ignoreCase = true)) return@forEach
                if (name.equals(HttpHeaders.ContentLength, ignoreCase = true)) return@forEach
                if (name.equals(HttpHeaders.TransferEncoding, ignoreCase = true)) return@forEach
                values.forEach { append(name, it) }
            }
        }
        return object : OutgoingContent.WriteChannelContent() {
            override val headers: Headers = filteredHeaders
            override suspend fun writeTo(channel: ByteWriteChannel) {
                val buffer = ByteArray(8192)
                while (!incoming.isClosedForRead) {
                    val read = incoming.readAvailable(buffer)
                    if (read < 0) break
                    val chunk = buffer.copyOf(read)
                    TrafficLogger.logRequestBodyChunk(chunk)
                    channel.writeFully(chunk, 0, read)
                    channel.flush()
                }
            }
        }
    }

    // Stream the upstream response back, logging text bodies to the console.
    private suspend fun respondWithBody(
        call: ApplicationCall,
        upstreamResponse: HttpResponse,
    ) {
        val responseContentType = upstreamResponse.contentType()
        val isText = responseContentType.isText

        call.respond(object : OutgoingContent.WriteChannelContent() {
            override val status: HttpStatusCode = upstreamResponse.status
            override val contentType: ContentType? = responseContentType
            override val headers: Headers = Headers.build {
                upstreamResponse.headers.forEach { name, values ->
                    if (name.equals(HttpHeaders.TransferEncoding, ignoreCase = true)) return@forEach
                    if (name.equals(HttpHeaders.ContentLength, ignoreCase = true)) return@forEach
                    values.forEach { append(name, it) }
                }
            }

            override suspend fun writeTo(channel: ByteWriteChannel) {
                val source = upstreamResponse.bodyAsChannel()
                val buffer = ByteArray(8192)
                while (!source.isClosedForRead) {
                    val read = source.readAvailable(buffer)
                    if (read < 0) break
                    val chunk = buffer.copyOf(read)
                    if (isText) {
                        TrafficLogger.logResponseBodyChunk(chunk)
                    }
                    channel.writeFully(chunk, 0, read)
                    channel.flush()
                }
            }
        })
    }

    // Determine if the content type is human-readable text.
    private val ContentType?.isText: Boolean
        get() = this?.let { ct ->
            ct.match(ContentType.Text.Any) ||
                ct.match(ContentType("text", "event-stream"))
        } ?: false
}
