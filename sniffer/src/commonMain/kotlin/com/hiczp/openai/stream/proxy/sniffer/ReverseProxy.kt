package com.hiczp.openai.stream.proxy.sniffer

import io.ktor.client.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.util.*
import io.ktor.utils.io.*
import kotlinx.io.readByteArray

class ReverseProxy(
    private val client: HttpClient,
    private val upstreamBaseUrl: String,
) {
    suspend fun forward(call: ApplicationCall) {
        val request = call.request
        val targetUrl = upstreamBaseUrl.trimEnd('/') + request.uri

        val requestBody = call.receiveChannel().readRemaining().readByteArray()
        TrafficLogger.logRequest(request.httpMethod, request.httpVersion, request.uri, request.headers, requestBody)

        val upstreamResponse = client.request(targetUrl) {
            method = request.httpMethod
            headers {
                appendAll(request.headers.filter { name, _ ->
                    !name.equals(HttpHeaders.Host, ignoreCase = true)
                })
            }
            setBody(requestBody)
        }

        val responseBody = upstreamResponse.bodyAsChannel().readRemaining().readByteArray()
        TrafficLogger.logResponse(
            request.uri,
            upstreamResponse.version,
            upstreamResponse.status,
            upstreamResponse.headers,
            responseBody
        )

        val isChunked = upstreamResponse.headers[HttpHeaders.TransferEncoding]
            ?.contains("chunked", ignoreCase = true) == true

        val filteredHeaders = Headers.build {
            appendAll(upstreamResponse.headers.filter { name, _ ->
                !name.equals(HttpHeaders.TransferEncoding, ignoreCase = true) &&
                        !name.equals(HttpHeaders.ContentLength, ignoreCase = true)
            })
        }

        if (isChunked) {
            call.respond(object : OutgoingContent.WriteChannelContent() {
                override val status: HttpStatusCode = upstreamResponse.status
                override val contentType: ContentType? = upstreamResponse.contentType()
                override val headers: Headers = filteredHeaders
                override suspend fun writeTo(channel: ByteWriteChannel) {
                    channel.writeFully(responseBody)
                }
            })
        } else {
            call.respond(object : OutgoingContent.ByteArrayContent() {
                override val status: HttpStatusCode = upstreamResponse.status
                override val contentType: ContentType? = upstreamResponse.contentType()
                override val headers: Headers = filteredHeaders
                override fun bytes(): ByteArray = responseBody
            })
        }
    }
}
