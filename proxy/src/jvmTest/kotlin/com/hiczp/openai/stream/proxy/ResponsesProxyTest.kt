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
import kotlin.test.assertFalse
import kotlin.test.assertNull
import io.ktor.server.cio.CIO as ServerCIO

class ResponsesProxyTest {
    private val sseResponseText: ByteArray by lazy {
        sseResource("responses_sse.txt")
    }

    private fun sseResource(fileName: String): ByteArray =
        javaClass.getResourceAsStream("/com/hiczp/openai/stream/proxy/$fileName")!!.readAllBytes()

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }

    private fun failedResponseSse(code: String): ByteArray {
        val data = buildJsonObject {
            put("type", "response.failed")
            put("response", buildJsonObject {
                put("id", "resp_failed")
                put("object", "response")
                put("status", "failed")
                put("error", buildJsonObject {
                    put("code", code)
                    put("message", "$code message")
                })
                putJsonArray("output") {}
            })
            put("sequence_number", 1)
        }
        return "event: response.failed\ndata: ${data}\n\n".encodeToByteArray()
    }

    private fun failedResponseWithoutErrorSse(): ByteArray {
        val data = buildJsonObject {
            put("type", "response.failed")
            put("response", buildJsonObject {
                put("id", "resp_failed")
                put("object", "response")
                put("status", "failed")
                putJsonArray("output") {}
            })
            put("sequence_number", 1)
        }
        return "event: response.failed\ndata: ${data}\n\n".encodeToByteArray()
    }

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
            routing { responsesProxyHandler(proxy) }
        }.start()

        try {
            block(downstreamPort)
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    private fun Route.responsesProxyHandler(proxy: ResponsesApiProxy) {
        post("/v1/responses") {
            try {
                proxy.proxy(
                    requestMethod = call.request.httpMethod,
                    requestUri = call.request.uri,
                    requestHeaders = call.request.headers,
                    requestBody = call.receiveChannel(),
                    respond = { call.respond(it) },
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
        }
    }

    private fun responsesRequest(model: String = "gpt-4", input: String = "hello", stream: Boolean? = null) =
        buildJsonObject {
            put("model", model)
            put("input", input)
            if (stream != null) put("stream", stream)
        }.toString()

    private suspend fun postResponse(
        downstreamPort: Int,
        body: String,
    ): Pair<HttpStatusCode, JsonObject> {
        val client = HttpClient(CIO.create())
        val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
            contentType(ContentType.Application.Json)
            setBody(body)
        }
        return response.status to Json.parseToJsonElement(response.bodyAsText()).jsonObject
    }

    @Test
    fun `proxy returns 200 for incomplete terminal response events`() = runBlocking {
        withProxyForSse(sseResource("responses_incomplete_sse.txt")) { downstreamPort ->
            val (status, body) = postResponse(downstreamPort, responsesRequest())

            assertEquals(HttpStatusCode.OK, status)
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
                val (status, body) = postResponse(downstreamPort, responsesRequest())
                val error = body.getValue("error").jsonObject

                assertEquals(case.expectedStatus, status)
                assertEquals(case.expectedType, error.getValue("type").jsonPrimitive.content)
                assertEquals(case.expectedCode, error.getValue("code").jsonPrimitive.content)
            }
        }
    }

    @Test
    fun `proxy passes through failed terminal response when error is missing`() = runBlocking {
        withProxyForSse(failedResponseWithoutErrorSse()) { downstreamPort ->
            val (status, body) = postResponse(downstreamPort, responsesRequest())

            assertEquals(HttpStatusCode.OK, status)
            assertEquals("failed", body["status"]?.jsonPrimitive?.content)
            assertFalse(body.containsKey("error"))
        }
    }

    @Test
    fun `proxy preserves upstream headers when failed terminal response is converted to error`() = runBlocking {
        val upstreamHeaders = Headers.build {
            append("X-Upstream-Request-Id", "req_failed")
            append("Retry-After", "3")
            append(HttpHeaders.Connection, "X-Remove-Me")
            append("X-Remove-Me", "should-not-forward")
            append("Keep-Alive", "timeout=5")
        }

        withProxyForSse(failedResponseSse("rate_limit_exceeded"), upstreamHeaders) { downstreamPort ->
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody(responsesRequest())
            }
            val bodyText = response.bodyAsText()

            assertEquals(HttpStatusCode.TooManyRequests, response.status)
            assertEquals("req_failed", response.headers["X-Upstream-Request-Id"])
            assertEquals("3", response.headers["Retry-After"])
            assertNull(response.headers["X-Remove-Me"])
            assertNull(response.headers["Keep-Alive"])
            assertEquals(
                listOf(bodyText.encodeToByteArray().size.toString()),
                response.headers.getAll(HttpHeaders.ContentLength),
            )
            assertEquals(
                listOf(ContentType.Application.Json.toString()),
                response.headers.getAll(HttpHeaders.ContentType),
            )
            assertEquals(ContentType.Application.Json, response.contentType()?.withoutParameters())
        }
    }

    @Test
    fun `proxy converts non-streaming request and aggregates SSE response`() = runBlocking {
        withProxyForSse(sseResponseText) { downstreamPort ->
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody(responsesRequest(model = "gpt-5.3-codex", input = "Hello"))
            }
            val body = Json.parseToJsonElement(response.bodyAsText()).jsonObject

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals(
                "resp_0da0bdeaf2fccdb9016a105f999e488191908828edc6db4c2a",
                body["id"]?.jsonPrimitive?.content,
            )
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
            routing { responsesProxyHandler(proxy) }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody(responsesRequest(model = "gpt-5.3-codex", input = "Hello", stream = true))
            }

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals(ContentType("text", "event-stream"), response.contentType()?.withoutParameters())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `proxy returns 502 for incomplete SSE stream`() = runBlocking {
        val incompleteSseData = (
                "event: response.output_item.done\n" +
                        "data: ${
                            buildJsonObject {
                                put("output_index", 0); put(
                                "item",
                                buildJsonObject { put("type", "message") })
                            }
                        }\n" +
                        "\n"
                ).toByteArray()

        withProxyForSse(incompleteSseData) { downstreamPort ->
            val (status, body) = postResponse(downstreamPort, responsesRequest())

            assertEquals(HttpStatusCode.BadGateway, status)
            val error = body.getValue("error").jsonObject
            assertEquals("upstream_error", error.getValue("type").jsonPrimitive.content)
        }
    }

    @Test
    fun `proxy returns 502 when upstream is unreachable`() = runBlocking {
        val downstreamPort = findFreePort()
        val unreachablePort = findFreePort()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$unreachablePort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { responsesProxyHandler(proxy) }
        }.start()

        try {
            val (status, body) = postResponse(downstreamPort, responsesRequest())

            assertEquals(HttpStatusCode.BadGateway, status)
            val error = body.getValue("error").jsonObject
            assertEquals("upstream_error", error.getValue("type").jsonPrimitive.content)
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
            routing { responsesProxyHandler(proxy) }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses?foo=bar&baz=123") {
                contentType(ContentType.Application.Json)
                setBody(responsesRequest())
            }
            assertEquals(HttpStatusCode.OK, response.status)
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
            routing { responsesProxyHandler(proxy) }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses?key=value") {
                contentType(ContentType.Application.Json)
                setBody(responsesRequest(stream = true))
            }
            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("/v1/responses?key=value", capturedUri.get())
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
            routing { responsesProxyHandler(proxy) }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                header("User-Agent", "TestClient/1.0")
                setBody(responsesRequest(model = "gpt-5.3-codex", input = "Hello"))
            }

            assertEquals("TestClient/1.0", capturedUserAgent.get())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `request headers are forwarded to upstream in passthrough flow`() = runBlocking {
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
            routing { responsesProxyHandler(proxy) }
        }.start()

        try {
            val client = HttpClient(CIO.create())
            client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                header("User-Agent", "TestClient/1.0")
                setBody(responsesRequest(model = "gpt-5.3-codex", input = "Hello", stream = true))
            }

            assertEquals("TestClient/1.0", capturedUserAgent.get())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }
}
