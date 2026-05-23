package com.hiczp.openai.stream.proxy

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
import kotlin.test.assertNotNull
import io.ktor.server.cio.CIO as ServerCIO

class ChatCompletionsProxyTest {
    private val sseResponseText: ByteArray by lazy {
        sseResource()
    }

    private fun sseResource(): ByteArray =
        javaClass.getResourceAsStream("/com/hiczp/openai/stream/proxy/chat_completions_sse.txt")!!.readAllBytes()

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }

    private suspend fun withProxyForSse(
        upstreamSseData: ByteArray,
        upstreamHeaders: Headers = Headers.Empty,
        block: suspend (downstreamPort: Int) -> Unit,
    ) {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/chat/completions") {
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override val headers = upstreamHeaders
                        override fun bytes(): ByteArray = upstreamSseData
                    })
                }
            }
        }.start()

        val proxy = ChatCompletionsApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { chatCompletionsProxyHandler(proxy) }
        }.start()

        try {
            block(downstreamPort)
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    private fun Route.chatCompletionsProxyHandler(proxy: ChatCompletionsApiProxy) {
        post("/v1/chat/completions") {
            val result = try {
                proxy.proxy(
                    requestMethod = call.request.httpMethod,
                    requestUri = call.request.uri,
                    requestHeaders = call.request.headers,
                    requestBody = call.receiveChannel(),
                )
            } catch (e: Exception) {
                return@post call.respond(
                    OpenAiErrors.errorResponse(
                        e.message ?: "Unknown error",
                        "proxy_error",
                        status = HttpStatusCode.InternalServerError,
                    )
                )
            }
            if (result != null) {
                call.respond(result)
            } else {
                call.respond(
                    OpenAiErrors.errorResponse("Upstream returned incomplete response", "upstream_error")
                )
            }
        }
    }

    private fun chatCompletionsRequest(model: String = "gpt-4", stream: Boolean? = null) =
        buildJsonObject {
            put("model", model)
            putJsonArray("messages") {
                add(buildJsonObject {
                    put("role", "user")
                    put("content", "Hello")
                })
            }
            if (stream != null) put("stream", stream)
        }.toString()

    private suspend fun postChatCompletion(
        downstreamPort: Int,
        body: String,
    ): Pair<HttpStatusCode, JsonObject> {
        val client = HttpClient(CIO.create())
        val response = client.post("http://127.0.0.1:$downstreamPort/v1/chat/completions") {
            contentType(ContentType.Application.Json)
            setBody(body)
        }
        return response.status to Json.parseToJsonElement(response.bodyAsText()).jsonObject
    }

    @Test
    fun `proxy converts non-streaming request and aggregates SSE response`() = runBlocking {
        withProxyForSse(sseResponseText) { downstreamPort ->
            val (status, body) = postChatCompletion(
                downstreamPort,
                chatCompletionsRequest(model = "gpt-5.4"),
            )

            assertEquals(HttpStatusCode.OK, status)
            assertEquals("chat.completion", body["object"]?.jsonPrimitive?.content)
            assertEquals(
                "chatcmpl-202605221412347747663898268d9d613pN8kA5",
                body["id"]?.jsonPrimitive?.content,
            )

            val choices = body["choices"]?.jsonArray
            assertNotNull(choices)
            assertEquals(1, choices.size)

            val choice = choices[0].jsonObject
            assertEquals(0, choice["index"]?.jsonPrimitive?.content?.toInt())
            assertEquals("stop", choice["finish_reason"]?.jsonPrimitive?.content)
            assertEquals("assistant", choice["message"]?.jsonObject?.get("role")?.jsonPrimitive?.content)
            assertEquals(
                "Hello! How can I help you today?",
                choice["message"]?.jsonObject?.get("content")?.jsonPrimitive?.content,
            )

            val usage = body["usage"]?.jsonObject
            assertNotNull(usage)
            assertEquals(7, usage["prompt_tokens"]?.jsonPrimitive?.content?.toInt())
            assertEquals(13, usage["completion_tokens"]?.jsonPrimitive?.content?.toInt())
            assertEquals(20, usage["total_tokens"]?.jsonPrimitive?.content?.toInt())
        }
    }

    @Test
    fun `passthrough when request already has stream true`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/chat/completions") {
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override fun bytes() = sseResponseText
                    })
                }
            }
        }.start()

        val proxy = ChatCompletionsApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { chatCompletionsProxyHandler(proxy) }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/chat/completions") {
                contentType(ContentType.Application.Json)
                setBody(chatCompletionsRequest(model = "gpt-5.4", stream = true))
            }

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals(ContentType("text", "event-stream"), response.contentType()?.withoutParameters())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `handler returns 502 when proxy returns null for incomplete SSE stream`() = runBlocking {
        val incompleteChunk = buildJsonObject {
            put("id", "chatcmpl-test")
            put("object", "chat.completion.chunk")
            putJsonArray("choices") {
                add(buildJsonObject {
                    putJsonObject("delta") { put("content", "Hi") }
                    put("index", 0)
                })
            }
        }
        val incompleteSseData = "data: $incompleteChunk\n\n".toByteArray()

        withProxyForSse(incompleteSseData) { downstreamPort ->
            val (status, body) = postChatCompletion(
                downstreamPort,
                chatCompletionsRequest(),
            )

            assertEquals(HttpStatusCode.BadGateway, status)
            val error = body.getValue("error").jsonObject
            assertEquals("upstream_error", error.getValue("type").jsonPrimitive.content)
        }
    }

    @Test
    fun `handler returns 500 when proxy throws exception`() = runBlocking {
        val downstreamPort = findFreePort()
        val unreachablePort = findFreePort()

        val proxy = ChatCompletionsApiProxy(CIO.create(), "http://127.0.0.1:$unreachablePort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { chatCompletionsProxyHandler(proxy) }
        }.start()

        try {
            val (status, body) = postChatCompletion(
                downstreamPort,
                chatCompletionsRequest(),
            )

            assertEquals(HttpStatusCode.InternalServerError, status)
            val error = body.getValue("error").jsonObject
            assertEquals("proxy_error", error.getValue("type").jsonPrimitive.content)
        } finally {
            downstreamServer.stop()
        }
    }

    @Test
    fun `query params are forwarded in convert flow`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val capturedUri = AtomicReference<String>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/chat/completions") {
                    capturedUri.set(call.request.uri)
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override fun bytes(): ByteArray = sseResponseText
                    })
                }
            }
        }.start()

        val proxy = ChatCompletionsApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { chatCompletionsProxyHandler(proxy) }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/chat/completions?foo=bar") {
                contentType(ContentType.Application.Json)
                setBody(chatCompletionsRequest())
            }
            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("/v1/chat/completions?foo=bar", capturedUri.get())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `query params are forwarded in passthrough flow`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val capturedUri = AtomicReference<String>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/chat/completions") {
                    capturedUri.set(call.request.uri)
                    call.respondText("ok", ContentType.Text.Plain)
                }
            }
        }.start()

        val proxy = ChatCompletionsApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { chatCompletionsProxyHandler(proxy) }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/chat/completions?key=value") {
                contentType(ContentType.Application.Json)
                setBody(chatCompletionsRequest(stream = true))
            }
            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("/v1/chat/completions?key=value", capturedUri.get())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `request headers are forwarded to upstream in convert flow`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val capturedUserAgent = AtomicReference<String>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/chat/completions") {
                    capturedUserAgent.set(call.request.headers["User-Agent"])
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override fun bytes(): ByteArray = sseResponseText
                    })
                }
            }
        }.start()

        val proxy = ChatCompletionsApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { chatCompletionsProxyHandler(proxy) }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            client.post("http://127.0.0.1:$downstreamPort/v1/chat/completions") {
                contentType(ContentType.Application.Json)
                header("User-Agent", "TestClient/1.0")
                setBody(chatCompletionsRequest())
            }

            assertEquals("TestClient/1.0", capturedUserAgent.get())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `upstream headers are preserved in convert flow`() = runBlocking {
        val upstreamHeaders = Headers.build {
            append("X-Request-Id", "req-123")
        }

        withProxyForSse(sseResponseText, upstreamHeaders) { downstreamPort ->
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/chat/completions") {
                contentType(ContentType.Application.Json)
                setBody(chatCompletionsRequest())
            }

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("req-123", response.headers["X-Request-Id"])
            assertEquals(ContentType.Application.Json, response.contentType()?.withoutParameters())
        }
    }

    @Test
    fun `non-matching path is passed through`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/other") {
                    call.respondText(buildJsonObject { put("ok", true) }.toString(), ContentType.Application.Json)
                }
            }
        }.start()

        val proxy = ChatCompletionsApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing {
                post("/v1/other") {
                    val result = proxy.proxy(
                        requestMethod = call.request.httpMethod,
                        requestUri = call.request.uri,
                        requestHeaders = call.request.headers,
                        requestBody = call.receiveChannel(),
                    )
                    if (result != null) call.respond(result) else call.respond(HttpStatusCode.BadGateway)
                }
            }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/other") {
                contentType(ContentType.Application.Json)
                setBody(buildJsonObject { put("model", "gpt-4") }.toString())
            }

            assertEquals(HttpStatusCode.OK, response.status)
            val body = Json.parseToJsonElement(response.bodyAsText()).jsonObject
            assertEquals(true, body["ok"]?.jsonPrimitive?.content?.toBoolean())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }
}
