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

    suspend fun forward(call: ApplicationCall) {
        val request = call.request
        val targetUrl = upstreamBaseUrl.trimEnd('/') + request.uri

        val requestBody = call.receiveChannel().readRemaining().readByteArray()
        TrafficLogger.logRequest(request.httpMethod, request.httpVersion, request.uri, request.headers, requestBody)

        val forwardHeaders = stripHopByHopHeaders(request.headers)

        val upstreamResponse = client.request(targetUrl) {
            method = request.httpMethod
            headers {
                appendAll(forwardHeaders)
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

        val filteredHeaders = Headers.build {
            appendAll(stripHopByHopHeaders(upstreamResponse.headers))
        }

        call.respond(object : OutgoingContent.ByteArrayContent() {
            override val status: HttpStatusCode = upstreamResponse.status
            override val contentType: ContentType? = upstreamResponse.contentType()
            override val headers: Headers = filteredHeaders
            override fun bytes(): ByteArray = responseBody
        })
    }
}
