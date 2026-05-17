package com.hiczp.openai.responses.stream.proxy

import com.openai.client.okhttp.OpenAIOkHttpClient
import com.openai.models.responses.EasyInputMessage
import com.openai.models.responses.ResponseCreateParams
import com.openai.models.responses.ResponseInputItem
import io.ktor.client.engine.cio.*
import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.server.engine.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.runBlocking
import java.net.ServerSocket
import kotlin.test.Test
import kotlin.test.assertEquals
import io.ktor.server.cio.CIO as ServerCIO

class ProxyTest {
    private val sseResponseText: ByteArray by lazy {
        javaClass.getResourceAsStream("/com/hiczp/openai/responses/stream/proxy/test_sse_response.txt")!!
            .readAllBytes()
    }

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }

    @Test
    fun `proxy converts non-streaming request and aggregates SSE response`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/responses") {
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override fun bytes(): ByteArray = sseResponseText
                    })
                }
            }
        }.start()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing {
                post("/v1/responses") {
                    val result = proxy.proxy(
                        requestMethod = call.request.httpMethod,
                        requestPath = call.request.uri,
                        requestHeaders = call.request.headers,
                        requestBody = call.receiveChannel(),
                    )
                    if (result != null) {
                        call.respond(result)
                    } else {
                        call.respond(HttpStatusCode.BadGateway)
                    }
                }
            }
        }.start()

        try {
            val openaiClient = OpenAIOkHttpClient.builder()
                .apiKey("test-key")
                .baseUrl("http://127.0.0.1:$downstreamPort/v1")
                .build()

            val message = EasyInputMessage.builder()
                .role(EasyInputMessage.Role.USER)
                .content("Hello")
                .build()

            val params = ResponseCreateParams.builder()
                .inputOfResponse(listOf(ResponseInputItem.ofEasyInputMessage(message)))
                .model("gpt-5.3-codex")
                .build()

            val response = openaiClient.responses().create(params)

            assertEquals(
                "resp_0e993bb721c78aa4016a089991a23c81919829e942dc1e9914",
                response.id(),
            )

            val text = response.output()
                .mapNotNull { it.message().orElse(null) }
                .flatMap { it.content() }
                .mapNotNull { it.outputText().orElse(null) }
                .joinToString("") { it.text() }
            assertEquals("Hi! 👋 How can I help you today?", text)
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `passthrough when request already has stream true`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/responses") {
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override fun bytes() = sseResponseText
                    })
                }
            }
        }.start()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing {
                post("/v1/responses") {
                    val result = proxy.proxy(
                        requestMethod = call.request.httpMethod,
                        requestPath = call.request.uri,
                        requestHeaders = call.request.headers,
                        requestBody = call.receiveChannel(),
                    )
                    if (result != null) {
                        call.respond(result)
                    } else {
                        call.respond(HttpStatusCode.BadGateway)
                    }
                }
            }
        }.start()

        try {
            val openaiClient = OpenAIOkHttpClient.builder()
                .apiKey("test-key")
                .baseUrl("http://127.0.0.1:$downstreamPort/v1")
                .build()

            val message = EasyInputMessage.builder()
                .role(EasyInputMessage.Role.USER)
                .content("Hello")
                .build()

            val params = ResponseCreateParams.builder()
                .inputOfResponse(listOf(ResponseInputItem.ofEasyInputMessage(message)))
                .model("gpt-5.3-codex")
                .build()

            val text = StringBuilder()
            openaiClient.responses().createStreaming(params).use { streamResponse ->
                streamResponse.stream().forEach { event ->
                    event.outputTextDelta().ifPresent { textEvent ->
                        text.append(textEvent.delta())
                    }
                }
            }

            assertEquals("Hi! 👋 How can I help you today?", text.toString())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }
}
