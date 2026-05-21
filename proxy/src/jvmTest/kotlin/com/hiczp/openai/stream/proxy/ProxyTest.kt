package com.hiczp.openai.stream.proxy

import com.openai.client.okhttp.OpenAIOkHttpClient
import com.openai.errors.OpenAIServiceException
import com.openai.models.responses.EasyInputMessage
import com.openai.models.responses.ResponseCreateParams
import com.openai.models.responses.ResponseInputItem
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
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.net.ServerSocket
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.*
import io.ktor.server.cio.CIO as ServerCIO

class ProxyTest {
    private val sseResponseText: ByteArray by lazy {
        sseResource("responses_sse.txt")
    }

    private fun sseResource(fileName: String): ByteArray =
        javaClass.getResourceAsStream("/com/hiczp/openai/stream/proxy/$fileName")!!.readAllBytes()

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }

    private fun failedResponseSse(code: String): ByteArray = buildString {
        append("event: response.failed\n")
        append("data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_failed\",\"object\":\"response\",\"status\":\"failed\",\"error\":{\"code\":\"$code\",\"message\":\"$code message\"},\"output\":[]},\"sequence_number\":1}\n")
        append('\n')
    }.encodeToByteArray()

    private fun failedResponseWithoutErrorSse(): ByteArray = buildString {
        append("event: response.failed\n")
        append("data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_failed\",\"object\":\"response\",\"status\":\"failed\",\"output\":[]},\"sequence_number\":1}\n")
        append('\n')
    }.encodeToByteArray()

    private suspend fun withProxyForSse(
        upstreamSseData: ByteArray,
        upstreamHeaders: Headers = Headers.Empty,
        block: suspend (downstreamPort: Int) -> Unit,
    ) {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/responses") {
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override val headers = upstreamHeaders
                        override fun bytes(): ByteArray = upstreamSseData
                    })
                }
            }
        }.start()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { proxyHandler(proxy) }
        }.start()

        try {
            block(downstreamPort)
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    private fun Route.proxyHandler(proxy: ResponsesApiProxy) {
        post("/v1/responses") {
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

    @Test
    fun `proxy returns 200 for incomplete terminal response events`() = runBlocking {
        withProxyForSse(sseResource("responses_incomplete_sse.txt")) { downstreamPort ->
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody("""{"model":"gpt-4","input":"hello"}""")
            }
            val body = Json.parseToJsonElement(response.bodyAsText()).jsonObject

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("incomplete", body["status"]?.jsonPrimitive?.content)
        }
    }

    @Test
    fun `proxy maps failed terminal response events to HTTP errors`() = runBlocking {
        data class Case(
            val sseData: ByteArray,
            val expectedStatus: HttpStatusCode,
            val expectedType: String,
            val expectedCode: String,
        )

        val cases = listOf(
            Case(
                sseResource("responses_failed_sse.txt"),
                HttpStatusCode.InternalServerError,
                "server_error",
                "server_error",
            ),
            Case(
                failedResponseSse("rate_limit_exceeded"),
                HttpStatusCode.TooManyRequests,
                "rate_limit_error",
                "rate_limit_exceeded",
            ),
            Case(
                failedResponseSse("server_error"),
                HttpStatusCode.InternalServerError,
                "server_error",
                "server_error",
            ),
            Case(
                failedResponseSse("vector_store_timeout"),
                HttpStatusCode.GatewayTimeout,
                "server_error",
                "vector_store_timeout",
            ),
            Case(
                failedResponseSse("invalid_prompt"),
                HttpStatusCode.BadRequest,
                "invalid_request_error",
                "invalid_prompt",
            ),
        )

        cases.forEach { case ->
            withProxyForSse(case.sseData) { downstreamPort ->
                val client = HttpClient(CIO.create())
                val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                    contentType(ContentType.Application.Json)
                    setBody("""{"model":"gpt-4","input":"hello"}""")
                }
                val error = Json.parseToJsonElement(response.bodyAsText()).jsonObject.getValue("error").jsonObject

                assertEquals(case.expectedStatus, response.status)
                assertEquals(case.expectedType, error.str("type"))
                assertEquals(case.expectedCode, error.str("code"))
            }
        }
    }

    @Test
    fun `proxy passes through failed terminal response when error is missing`() = runBlocking {
        withProxyForSse(failedResponseWithoutErrorSse()) { downstreamPort ->
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody("""{"model":"gpt-4","input":"hello"}""")
            }
            val body = Json.parseToJsonElement(response.bodyAsText()).jsonObject

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("failed", body["status"]?.jsonPrimitive?.content)
            assertFalse(body.containsKey("error"))
        }
    }

    @Test
    fun `proxy preserves upstream headers when failed terminal response is converted to error`() = runBlocking {
        val upstreamHeaders = Headers.build {
            append("X-Upstream-Request-Id", "req_failed")
            append("Retry-After", "3")
        }

        withProxyForSse(failedResponseSse("rate_limit_exceeded"), upstreamHeaders) { downstreamPort ->
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody("""{"model":"gpt-4","input":"hello"}""")
            }

            assertEquals(HttpStatusCode.TooManyRequests, response.status)
            assertEquals("req_failed", response.headers["X-Upstream-Request-Id"])
            assertEquals("3", response.headers["Retry-After"])
            assertEquals(ContentType.Application.Json, response.contentType()?.withoutParameters())
        }
    }

    @Test
    fun `openai SDK sees failed rate limit response as 429 service exception`() = runBlocking {
        withProxyForSse(failedResponseSse("rate_limit_exceeded")) { downstreamPort ->
            val openaiClient = OpenAIOkHttpClient.builder()
                .apiKey("test-key")
                .baseUrl("http://127.0.0.1:$downstreamPort/v1")
                .maxRetries(0)
                .build()

            val message = EasyInputMessage.builder()
                .role(EasyInputMessage.Role.USER)
                .content("Hello")
                .build()

            val params = ResponseCreateParams.builder()
                .inputOfResponse(listOf(ResponseInputItem.ofEasyInputMessage(message)))
                .model("gpt-4")
                .build()

            val exception = assertFailsWith<OpenAIServiceException> {
                openaiClient.responses().create(params)
            }
            assertEquals(429, exception.statusCode())
        }
    }

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
            routing { proxyHandler(proxy) }
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
            routing { proxyHandler(proxy) }
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

    @Test
    fun `handler returns 502 when proxy returns null for incomplete SSE stream`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()

        val incompleteSseData = (
                "event: response.output_item.done\n" +
                        "data: {\"output_index\":0,\"item\":{\"type\":\"message\"}}\n" +
                        "\n"
                ).toByteArray()

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
            routing { proxyHandler(proxy) }
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
                .model("gpt-4")
                .build()

            val exception = assertFailsWith<OpenAIServiceException> {
                openaiClient.responses().create(params)
            }
            assertEquals(502, exception.statusCode())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `handler returns 500 when proxy throws exception`() = runBlocking {
        val downstreamPort = findFreePort()
        val unreachablePort = findFreePort()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$unreachablePort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { proxyHandler(proxy) }
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
                .model("gpt-4")
                .build()

            val exception = assertFailsWith<OpenAIServiceException> {
                openaiClient.responses().create(params)
            }
            assertEquals(500, exception.statusCode())
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
                post("/v1/responses") {
                    capturedUri.set(call.request.uri)
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override fun bytes(): ByteArray = sseResponseText
                    })
                }
            }
        }.start()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { proxyHandler(proxy) }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses?foo=bar&baz=123") {
                contentType(ContentType.Application.Json)
                setBody("""{"model":"gpt-4","input":"hello"}""")
            }
            assertEquals(200, response.status.value)
            assertEquals("/v1/responses?foo=bar&baz=123", capturedUri.get())
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
                post("/v1/responses") {
                    capturedUri.set(call.request.uri)
                    call.respondText("ok", ContentType.Text.Plain)
                }
            }
        }.start()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { proxyHandler(proxy) }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses?key=value") {
                contentType(ContentType.Application.Json)
                setBody("""{"model":"gpt-4","input":"hello","stream":true}""")
            }
            assertEquals(200, response.status.value)
            assertEquals("/v1/responses?key=value", capturedUri.get())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `openai SDK User-Agent is forwarded to upstream in convert flow`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val capturedUserAgent = AtomicReference<String>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/responses") {
                    capturedUserAgent.set(call.request.headers["User-Agent"])
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override fun bytes(): ByteArray = sseResponseText
                    })
                }
            }
        }.start()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { proxyHandler(proxy) }
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

            openaiClient.responses().create(params)

            val ua = capturedUserAgent.get()
            assertNotNull(ua, "Upstream should receive a User-Agent header")
            assertTrue(
                ua.contains("OpenAI"),
                "Expected User-Agent to contain 'OpenAI' (SDK UA), but got: $ua"
            )
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `openai SDK User-Agent is forwarded to upstream in passthrough flow`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val capturedUserAgent = AtomicReference<String>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/responses") {
                    capturedUserAgent.set(call.request.headers["User-Agent"])
                    call.respond(object : OutgoingContent.ByteArrayContent() {
                        override val contentType = ContentType("text", "event-stream")
                        override fun bytes() = sseResponseText
                    })
                }
            }
        }.start()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { proxyHandler(proxy) }
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

            openaiClient.responses().createStreaming(params).use { streamResponse ->
                streamResponse.stream().forEach { }
            }

            val ua = capturedUserAgent.get()
            assertNotNull(ua, "Upstream should receive a User-Agent header")
            assertTrue(
                ua.contains("OpenAI"),
                "Expected User-Agent to contain 'OpenAI' (SDK UA), but got: $ua"
            )
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }
}

private fun JsonObject.str(name: String) = getValue(name).jsonPrimitive.content
