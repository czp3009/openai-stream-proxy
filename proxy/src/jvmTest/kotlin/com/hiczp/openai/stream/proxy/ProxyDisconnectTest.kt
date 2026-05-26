package com.hiczp.openai.stream.proxy

import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.engine.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.*
import kotlinx.serialization.json.*
import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds
import io.ktor.server.cio.CIO as ServerCIO

class ProxyDisconnectTest {
    private class ApiCase(
        val name: String,
        val path: String,
        val requestBody: String,
        val partialSseData: ByteArray,
        val createProxy: (HttpClientEngine, String) -> AbstractApiProxy,
    )

    private enum class RawUpstreamFailure {
        CloseAfterAccept,
        CloseAfterRequestHeaders,
        CloseAfterStatusLine,
        CloseAfterSseHeaders,
        CloseAfterPartialSseEvent,
    }

    private data class RawUpstreamSignals(
        val accepted: CompletableDeferred<Unit> = CompletableDeferred(),
        val requestHeadersRead: CompletableDeferred<Unit> = CompletableDeferred(),
        val statusLineWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val responseHeadersWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val partialBodyWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val disconnected: CompletableDeferred<Unit> = CompletableDeferred(),
    ) {
        fun completeExceptionally(cause: Throwable) {
            accepted.completeExceptionally(cause)
            requestHeadersRead.completeExceptionally(cause)
            statusLineWritten.completeExceptionally(cause)
            responseHeadersWritten.completeExceptionally(cause)
            partialBodyWritten.completeExceptionally(cause)
            disconnected.completeExceptionally(cause)
        }
    }

    private val apiCases = listOf(
        ApiCase(
            name = "chat-completions",
            path = "/v1/chat/completions",
            requestBody = buildJsonObject {
                put("model", "gpt-4")
                putJsonArray("messages") {
                    add(buildJsonObject {
                        put("role", "user")
                        put("content", "Hello")
                    })
                }
            }.toString(),
            partialSseData = ("data: " + buildJsonObject {
                put("id", "chatcmpl-test")
                put("object", "chat.completion.chunk")
                putJsonArray("choices") {
                    add(buildJsonObject {
                        putJsonObject("delta") { put("content", "Hi") }
                        put("index", 0)
                    })
                }
            } + "\n\n").encodeToByteArray(),
            createProxy = { engine, upstreamBaseUrl -> ChatCompletionsApiProxy(engine, upstreamBaseUrl) },
        ),
        ApiCase(
            name = "responses",
            path = "/v1/responses",
            requestBody = buildJsonObject {
                put("model", "gpt-4")
                put("input", "Hello")
            }.toString(),
            partialSseData = ("event: response.output_item.done\n" + "data: " + buildJsonObject {
                put("output_index", 0)
                putJsonObject("item") { put("type", "message") }
            } + "\n\n").encodeToByteArray(),
            createProxy = { engine, upstreamBaseUrl -> ResponsesApiProxy(engine, upstreamBaseUrl) },
        ),
    )

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }

    private suspend fun withFailingRawUpstream(
        apiCase: ApiCase,
        failure: RawUpstreamFailure,
        block: suspend CoroutineScope.(downstreamPort: Int, signals: RawUpstreamSignals) -> Unit,
    ): Unit = coroutineScope {
        val upstreamSocket = ServerSocket(0)
        val upstreamPort = upstreamSocket.localPort
        val downstreamPort = findFreePort()
        val signals = RawUpstreamSignals()
        val upstreamJob = async(Dispatchers.IO) {
            try {
                upstreamSocket.use { serverSocket ->
                    serverSocket.accept().use { socket ->
                        signals.accepted.complete(Unit)

                        if (failure != RawUpstreamFailure.CloseAfterAccept) {
                            readHttpRequestHeaders(socket.getInputStream())
                            signals.requestHeadersRead.complete(Unit)
                        }

                        val output = socket.getOutputStream()
                        when (failure) {
                            RawUpstreamFailure.CloseAfterAccept,
                            RawUpstreamFailure.CloseAfterRequestHeaders -> Unit

                            RawUpstreamFailure.CloseAfterStatusLine -> {
                                writeStatusLine(output)
                                signals.statusLineWritten.complete(Unit)
                            }

                            RawUpstreamFailure.CloseAfterSseHeaders -> {
                                writeSseResponseHeaders(output)
                                signals.responseHeadersWritten.complete(Unit)
                            }

                            RawUpstreamFailure.CloseAfterPartialSseEvent -> {
                                writeSseResponseHeaders(output)
                                signals.responseHeadersWritten.complete(Unit)
                                output.write(apiCase.partialSseData)
                                output.flush()
                                signals.partialBodyWritten.complete(Unit)
                            }
                        }
                    }
                }
                signals.disconnected.complete(Unit)
            } catch (e: Throwable) {
                signals.completeExceptionally(e)
                throw e
            }
        }

        val engine = CIO.create()
        val proxy = apiCase.createProxy(engine, "http://127.0.0.1:$upstreamPort")
        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { proxyHandler(proxy) }
        }.start()

        try {
            block(downstreamPort, signals)
            upstreamJob.await()
        } finally {
            downstreamServer.stop()
            engine.close()
            upstreamSocket.close()
            if (!upstreamJob.isCompleted) {
                upstreamJob.cancelAndJoin()
            }
        }
    }

    private suspend fun withRawHttpResponseUpstream(
        apiCase: ApiCase,
        rawResponse: ByteArray,
        block: suspend (downstreamPort: Int) -> Unit,
    ): Unit = coroutineScope {
        val upstreamSocket = ServerSocket(0)
        val upstreamPort = upstreamSocket.localPort
        val downstreamPort = findFreePort()
        val upstreamJob = async(Dispatchers.IO) {
            upstreamSocket.use { serverSocket ->
                serverSocket.accept().use { socket ->
                    readHttpRequestHeaders(socket.getInputStream())
                    socket.getOutputStream().write(rawResponse)
                    socket.getOutputStream().flush()
                }
            }
        }

        val engine = CIO.create()
        val proxy = apiCase.createProxy(engine, "http://127.0.0.1:$upstreamPort")
        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { proxyHandler(proxy) }
        }.start()

        try {
            block(downstreamPort)
            upstreamJob.await()
        } finally {
            downstreamServer.stop()
            engine.close()
            upstreamSocket.close()
            if (!upstreamJob.isCompleted) {
                upstreamJob.cancelAndJoin()
            }
        }
    }

    private fun Route.proxyHandler(proxy: AbstractApiProxy) {
        route("/{...}") {
            handle {
                try {
                    proxy.proxy(
                        requestMethod = call.request.httpMethod,
                        requestUri = call.request.uri,
                        requestHeaders = call.request.headers,
                        requestBody = call.receiveChannel(),
                        respond = { call.respond(it) },
                    )
                } catch (e: Exception) {
                    call.respond(
                        OpenAiErrors.errorResponse(
                            message = e.message ?: "Unknown error",
                            type = "proxy_error",
                            status = HttpStatusCode.InternalServerError,
                        )
                    )
                }
            }
        }
    }

    private suspend fun postApiRequest(
        downstreamPort: Int,
        apiCase: ApiCase,
    ): Pair<HttpStatusCode, JsonObject> {
        val client = HttpClient(CIO.create())
        return try {
            val response = client.post("http://127.0.0.1:$downstreamPort${apiCase.path}") {
                contentType(ContentType.Application.Json)
                setBody(apiCase.requestBody)
            }
            response.status to Json.parseToJsonElement(response.bodyAsText()).jsonObject
        } finally {
            client.close()
        }
    }

    private suspend fun postApiRequestDetails(
        downstreamPort: Int,
        apiCase: ApiCase,
    ): Triple<HttpStatusCode, Headers, String> {
        val client = HttpClient(CIO.create())
        return try {
            val response = client.post("http://127.0.0.1:$downstreamPort${apiCase.path}") {
                contentType(ContentType.Application.Json)
                setBody(apiCase.requestBody)
            }
            Triple(response.status, response.headers, response.bodyAsText())
        } finally {
            client.close()
        }
    }

    private fun readHttpRequestHeaders(input: InputStream) {
        val delimiter = "\r\n\r\n".encodeToByteArray()
        var matched = 0
        while (matched < delimiter.size) {
            val byte = input.read()
            if (byte == -1) return
            matched = if (byte.toByte() == delimiter[matched]) {
                matched + 1
            } else if (byte.toByte() == delimiter[0]) {
                1
            } else {
                0
            }
        }
    }

    private fun writeStatusLine(output: OutputStream) {
        output.write("HTTP/1.1 200 OK\r\n".encodeToByteArray())
        output.flush()
    }

    private fun writeSseResponseHeaders(output: OutputStream) {
        val headers = "HTTP/1.1 200 OK\r\n" +
                "Content-Type: text/event-stream\r\n" +
                "Connection: close\r\n" +
                "\r\n"
        output.write(headers.encodeToByteArray())
        output.flush()
    }

    @Test
    fun `convert flow returns 502 for deterministic upstream disconnect points`() = runBlocking {
        apiCases.forEach { apiCase ->
            RawUpstreamFailure.entries.forEach { failure ->
                withFailingRawUpstream(apiCase, failure) { downstreamPort, signals ->
                    val downstreamResponse = async {
                        postApiRequest(downstreamPort, apiCase)
                    }

                    withTimeout(2.seconds) {
                        signals.accepted.await()
                        when (failure) {
                            RawUpstreamFailure.CloseAfterAccept -> Unit
                            RawUpstreamFailure.CloseAfterRequestHeaders -> signals.requestHeadersRead.await()
                            RawUpstreamFailure.CloseAfterStatusLine -> signals.statusLineWritten.await()
                            RawUpstreamFailure.CloseAfterSseHeaders -> signals.responseHeadersWritten.await()
                            RawUpstreamFailure.CloseAfterPartialSseEvent -> signals.partialBodyWritten.await()
                        }
                        signals.disconnected.await()
                    }

                    val (status, body) = withTimeout(2.seconds) { downstreamResponse.await() }
                    assertEquals(HttpStatusCode.BadGateway, status, "${apiCase.name} $failure")
                    val error = body.getValue("error").jsonObject
                    assertEquals(
                        "upstream_error",
                        error.getValue("type").jsonPrimitive.content,
                        "${apiCase.name} $failure"
                    )
                }
            }
        }
    }

    @Test
    fun `convert flow relays non-SSE upstream error responses`() = runBlocking {
        val upstreamBody = buildJsonObject {
            putJsonObject("error") {
                put("message", "rate limited")
                put("type", "rate_limit_error")
            }
        }.toString()
        val rawResponse = (
                "HTTP/1.1 429 Too Many Requests\r\n" +
                        "Content-Type: application/json\r\n" +
                        "Content-Length: ${upstreamBody.encodeToByteArray().size}\r\n" +
                        "X-Upstream-Request-Id: req_edge\r\n" +
                        "Connection: close\r\n" +
                        "\r\n" +
                        upstreamBody
                ).encodeToByteArray()

        apiCases.forEach { apiCase ->
            withRawHttpResponseUpstream(apiCase, rawResponse) { downstreamPort ->
                val (status, headers, body) = postApiRequestDetails(downstreamPort, apiCase)
                assertEquals(HttpStatusCode.TooManyRequests, status, apiCase.name)
                assertEquals("req_edge", headers["X-Upstream-Request-Id"], apiCase.name)
                assertEquals(upstreamBody, body, apiCase.name)
            }
        }
    }
}
