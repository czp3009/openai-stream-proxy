package com.hiczp.openai.stream.proxy.cli

import com.hiczp.openai.stream.proxy.ChatCompletionsApiProxy
import com.hiczp.openai.stream.proxy.PassthroughApiProxy
import com.hiczp.openai.stream.proxy.ResponsesApiProxy
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.server.engine.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import io.ktor.server.cio.CIO as ServerCIO

class ServerTest {
    private val responsesSseResponseText: ByteArray by lazy {
        javaClass.getResourceAsStream("/com/hiczp/openai/stream/proxy/cli/responses_sse.txt")!!.readAllBytes()
    }

    private val chatCompletionsSseResponseText: ByteArray by lazy {
        javaClass.getResourceAsStream("/com/hiczp/openai/stream/proxy/cli/chat_completions_sse.txt")!!.readAllBytes()
    }

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }

    private fun eventStreamContent(body: ByteArray) = object : OutgoingContent.ByteArrayContent() {
        override val contentType = ContentType("text", "event-stream")
        override fun bytes(): ByteArray = body
    }

    private fun startProxyServer(downstreamPort: Int, upstreamPort: Int) =
        embeddedServer(ServerCIO, port = downstreamPort) {
            configureProxyServer(
                CIO.create(),
                listOf(ProxyRule(downstreamPort, "http://127.0.0.1:$upstreamPort")),
                600_000L,
            )
        }.start()

    @Test
    fun `selects proxy implementation by request path`() {
        assertEquals(ResponsesApiProxy::class, selectProxyClass("/v1/responses"))
        assertEquals(ChatCompletionsApiProxy::class, selectProxyClass("/v1/chat/completions"))
        assertEquals(PassthroughApiProxy::class, selectProxyClass("/v1/models"))
    }

    @Test
    fun `configureProxyServer routes responses path to responses proxy`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val capturedBody = AtomicReference<String>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/responses") {
                    capturedBody.set(call.receiveText())
                    call.respond(eventStreamContent(responsesSseResponseText))
                }
            }
        }.start()
        val downstreamServer = startProxyServer(downstreamPort, upstreamPort)

        val client = HttpClient(CIO)
        try {
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody(buildJsonObject { put("model", "gpt-4"); put("input", "hello") }.toString())
            }

            assertEquals(HttpStatusCode.OK, response.status)
            val responseBody = Json.parseToJsonElement(response.bodyAsText()).jsonObject
            assertEquals("response", responseBody.getValue("object").jsonPrimitive.content)
            assertEquals("completed", responseBody.getValue("status").jsonPrimitive.content)

            val forwardedBody = Json.parseToJsonElement(
                capturedBody.get() ?: error("Upstream did not receive responses request"),
            ).jsonObject
            assertEquals(true, forwardedBody.getValue("stream").jsonPrimitive.boolean)
        } finally {
            client.close()
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `configureProxyServer routes chat completions path to chat completions proxy`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val capturedBody = AtomicReference<String>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/chat/completions") {
                    capturedBody.set(call.receiveText())
                    call.respond(eventStreamContent(chatCompletionsSseResponseText))
                }
            }
        }.start()
        val downstreamServer = startProxyServer(downstreamPort, upstreamPort)

        val client = HttpClient(CIO)
        try {
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/chat/completions") {
                contentType(ContentType.Application.Json)
                setBody("""{"model":"gpt-4","messages":[{"role":"user","content":"hello"}]}""")
            }

            assertEquals(HttpStatusCode.OK, response.status)
            val responseBody = Json.parseToJsonElement(response.bodyAsText()).jsonObject
            assertEquals("chat.completion", responseBody.getValue("object").jsonPrimitive.content)
            assertEquals(
                "stop",
                responseBody.getValue("choices").jsonArray[0].jsonObject
                    .getValue("finish_reason").jsonPrimitive.content,
            )

            val forwardedBody = Json.parseToJsonElement(
                capturedBody.get() ?: error("Upstream did not receive chat completions request"),
            ).jsonObject
            assertEquals(true, forwardedBody.getValue("stream").jsonPrimitive.boolean)
            assertEquals(
                true,
                forwardedBody.getValue("stream_options").jsonObject
                    .getValue("include_usage").jsonPrimitive.boolean,
            )
        } finally {
            client.close()
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `configureProxyServer routes unmatched path to passthrough proxy`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val capturedBody = AtomicReference<String>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/models") {
                    capturedBody.set(call.receiveText())
                    call.respondText("""{"mode":"passthrough"}""", ContentType.Application.Json)
                }
            }
        }.start()
        val downstreamServer = startProxyServer(downstreamPort, upstreamPort)

        val client = HttpClient(CIO)
        try {
            val originalBody = buildJsonObject {
                put("model", "gpt-4")
                put("stream", false)
            }.toString()
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/models") {
                contentType(ContentType.Application.Json)
                setBody(originalBody)
            }

            assertEquals(HttpStatusCode.OK, response.status)
            val responseBody = Json.parseToJsonElement(response.bodyAsText()).jsonObject
            assertEquals("passthrough", responseBody.getValue("mode").jsonPrimitive.content)

            val forwardedBody = capturedBody.get() ?: error("Upstream did not receive passthrough request")
            assertEquals(Json.parseToJsonElement(originalBody), Json.parseToJsonElement(forwardedBody))
        } finally {
            client.close()
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `proxy returns aggregated response for valid SSE upstream`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/responses") {
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override fun bytes(): ByteArray = responsesSseResponseText
                    })
                }
            }
        }.start()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            installErrorHandler()
            routing {
                route("/{...}") {
                    handle {
                        proxy.proxy(
                            requestMethod = call.request.httpMethod,
                            requestUri = call.request.uri,
                            requestHeaders = call.request.headers,
                            requestBody = call.receiveChannel(),
                            respond = { call.respond(it) },
                        )
                    }
                }
            }
        }.start()

        val client = HttpClient(CIO)
        try {
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody(buildJsonObject { put("model", "gpt-4"); put("input", "hello") }.toString())
            }
            assertEquals(HttpStatusCode.OK, response.status)
            val body = response.bodyAsText()
            assertTrue(body.contains("resp_"))
        } finally {
            client.close()
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `returns 502 when upstream SSE stream is incomplete`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()

        val incompleteChunk = buildJsonObject {
            put("output_index", 0)
            putJsonObject("item") { put("type", "message") }
        }
        val incompleteSseData = "event: response.output_item.done\ndata: $incompleteChunk\n\n".toByteArray()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/responses") {
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override fun bytes() = incompleteSseData
                    })
                }
            }
        }.start()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            installErrorHandler()
            routing {
                route("/{...}") {
                    handle {
                        proxy.proxy(
                            requestMethod = call.request.httpMethod,
                            requestUri = call.request.uri,
                            requestHeaders = call.request.headers,
                            requestBody = call.receiveChannel(),
                            respond = { call.respond(it) },
                        )
                    }
                }
            }
        }.start()

        val client = HttpClient(CIO)
        try {
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody(buildJsonObject { put("model", "gpt-4"); put("input", "hello") }.toString())
            }
            assertEquals(HttpStatusCode.BadGateway, response.status)
            val body = response.bodyAsText()
            assertTrue(body.contains("upstream_error"))
        } finally {
            client.close()
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `returns 502 when upstream is unreachable`() = runBlocking {
        val downstreamPort = findFreePort()
        val unreachablePort = findFreePort()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$unreachablePort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            installErrorHandler()
            routing {
                route("/{...}") {
                    handle {
                        proxy.proxy(
                            requestMethod = call.request.httpMethod,
                            requestUri = call.request.uri,
                            requestHeaders = call.request.headers,
                            requestBody = call.receiveChannel(),
                            respond = { call.respond(it) },
                        )
                    }
                }
            }
        }.start()

        val client = HttpClient(CIO)
        try {
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody(buildJsonObject { put("model", "gpt-4"); put("input", "hello") }.toString())
            }
            assertEquals(HttpStatusCode.BadGateway, response.status)
            val body = response.bodyAsText()
            assertTrue(body.contains("upstream_error"))
        } finally {
            client.close()
            downstreamServer.stop()
        }
    }

    @Test
    fun `routes requests to correct upstream based on port`() = runBlocking {
        val upstreamPortA = findFreePort()
        val upstreamPortB = findFreePort()
        val portA = findFreePort()
        val portB = findFreePort()

        val upstreamA = embeddedServer(ServerCIO, port = upstreamPortA) {
            routing { get("/identify") { call.respondText("upstream-A") } }
        }.start()
        val upstreamB = embeddedServer(ServerCIO, port = upstreamPortB) {
            routing { get("/identify") { call.respondText("upstream-B") } }
        }.start()

        val rules = listOf(
            ProxyRule(portA, "http://127.0.0.1:$upstreamPortA"),
            ProxyRule(portB, "http://127.0.0.1:$upstreamPortB"),
        )

        val server = embeddedServer(
            ServerCIO,
            configure = { rules.forEach { connector { port = it.listenPort } } }
        ) { configureProxyServer(CIO.create(), rules, 600_000L) }.start()

        val client = HttpClient(CIO)
        try {
            assertEquals("upstream-A", client.get("http://127.0.0.1:$portA/identify").bodyAsText())
            assertEquals("upstream-B", client.get("http://127.0.0.1:$portB/identify").bodyAsText())
        } finally {
            client.close()
            server.stop()
            upstreamA.stop()
            upstreamB.stop()
        }
    }
}
