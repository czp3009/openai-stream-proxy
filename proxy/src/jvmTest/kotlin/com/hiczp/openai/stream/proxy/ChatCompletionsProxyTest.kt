package com.hiczp.openai.stream.proxy

import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.sse.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.sse.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.take
import kotlinx.serialization.json.*
import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.util.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds
import io.ktor.client.plugins.sse.SSE as ClientSSE
import io.ktor.server.cio.CIO as ServerCIO
import io.ktor.server.sse.SSE as ServerSSE

class ChatCompletionsProxyTest {
    private val sseResponseText: ByteArray by lazy {
        sseResource()
    }

    private fun sseResource(): ByteArray =
        javaClass.getResourceAsStream("/com/hiczp/openai/stream/proxy/chat_completions_sse.txt")!!.readAllBytes()

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }

    private data class DisconnectingSseUpstreamSignals(
        val accepted: CompletableDeferred<Unit> = CompletableDeferred(),
        val requestHeadersRead: CompletableDeferred<Unit> = CompletableDeferred(),
        val responseHeadersWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val sseBodyWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val disconnected: CompletableDeferred<Unit> = CompletableDeferred(),
    ) {
        fun completeExceptionally(cause: Throwable) {
            accepted.completeExceptionally(cause)
            requestHeadersRead.completeExceptionally(cause)
            responseHeadersWritten.completeExceptionally(cause)
            sseBodyWritten.completeExceptionally(cause)
            disconnected.completeExceptionally(cause)
        }
    }

    private data class GatedPassthroughRawSseUpstreamSignals(
        val accepted: CompletableDeferred<Unit> = CompletableDeferred(),
        val requestHeadersRead: CompletableDeferred<Unit> = CompletableDeferred(),
        val responseHeadersWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val firstEventWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val disconnectRequested: CompletableDeferred<Unit> = CompletableDeferred(),
        val disconnected: CompletableDeferred<Unit> = CompletableDeferred(),
    ) {
        fun completeExceptionally(cause: Throwable) {
            accepted.completeExceptionally(cause)
            requestHeadersRead.completeExceptionally(cause)
            responseHeadersWritten.completeExceptionally(cause)
            firstEventWritten.completeExceptionally(cause)
            disconnectRequested.completeExceptionally(cause)
            disconnected.completeExceptionally(cause)
        }
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

    private suspend fun withDisconnectingSseUpstream(
        upstreamSseData: ByteArray,
        block: suspend CoroutineScope.(downstreamPort: Int, signals: DisconnectingSseUpstreamSignals) -> Unit,
    ): Unit = coroutineScope {
        val upstreamSocket = ServerSocket(0)
        val upstreamPort = upstreamSocket.localPort
        val downstreamPort = findFreePort()
        val signals = DisconnectingSseUpstreamSignals()
        val upstreamJob = async(Dispatchers.IO) {
            try {
                upstreamSocket.use { serverSocket ->
                    serverSocket.accept().use { socket ->
                        signals.accepted.complete(Unit)
                        readHttpRequestHeaders(socket.getInputStream())
                        signals.requestHeadersRead.complete(Unit)
                        writeSseResponseAndDisconnect(socket.getOutputStream(), upstreamSseData, signals)
                    }
                }
                signals.disconnected.complete(Unit)
            } catch (e: Throwable) {
                signals.completeExceptionally(e)
                throw e
            }
        }

        val proxy = ChatCompletionsApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { chatCompletionsProxyHandler(proxy) }
        }.start()

        try {
            block(downstreamPort, signals)
            upstreamJob.await()
        } finally {
            downstreamServer.stop()
            upstreamSocket.close()
            if (!upstreamJob.isCompleted) {
                upstreamJob.cancelAndJoin()
            }
        }
    }

    private suspend fun withGatedPassthroughRawSseUpstream(
        writeResponseHeaders: Boolean,
        writeFirstEvent: Boolean = false,
        events: MutableList<String>? = null,
        block: suspend CoroutineScope.(downstreamPort: Int, signals: GatedPassthroughRawSseUpstreamSignals) -> Unit,
    ): Unit = coroutineScope {
        val upstreamSocket = ServerSocket(0)
        val upstreamPort = upstreamSocket.localPort
        val downstreamPort = findFreePort()
        val signals = GatedPassthroughRawSseUpstreamSignals()
        val upstreamJob = async(Dispatchers.IO) {
            try {
                upstreamSocket.use { serverSocket ->
                    serverSocket.accept().use { socket ->
                        signals.accepted.complete(Unit)
                        readHttpRequestHeaders(socket.getInputStream())
                        signals.requestHeadersRead.complete(Unit)

                        val output = socket.getOutputStream()
                        if (writeResponseHeaders) {
                            writeRawSseResponseHeaders(output)
                            events?.add("upstream-headers")
                            signals.responseHeadersWritten.complete(Unit)
                        }
                        if (writeFirstEvent) {
                            output.write("data: first\n\n".encodeToByteArray())
                            output.flush()
                            events?.add("upstream-1")
                            signals.firstEventWritten.complete(Unit)
                        }

                        signals.disconnectRequested.await()
                    }
                }
                events?.add("upstream-disconnect")
                signals.disconnected.complete(Unit)
            } catch (e: Throwable) {
                signals.completeExceptionally(e)
                throw e
            }
        }

        val proxy = ChatCompletionsApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { chatCompletionsProxyHandler(proxy) }
        }.start()

        try {
            block(downstreamPort, signals)
            signals.disconnectRequested.complete(Unit)
            upstreamJob.await()
        } finally {
            signals.disconnectRequested.complete(Unit)
            downstreamServer.stop()
            upstreamSocket.close()
            if (!upstreamJob.isCompleted) {
                upstreamJob.cancelAndJoin()
            }
        }
    }

    private suspend fun withRawHttpUpstreamResponse(
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

        val proxy = ChatCompletionsApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { chatCompletionsAndOtherProxyHandler(proxy) }
        }.start()

        try {
            block(downstreamPort)
            upstreamJob.await()
        } finally {
            downstreamServer.stop()
            upstreamSocket.close()
            if (!upstreamJob.isCompleted) {
                upstreamJob.cancelAndJoin()
            }
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

    private fun writeSseResponseAndDisconnect(
        output: OutputStream,
        body: ByteArray,
        signals: DisconnectingSseUpstreamSignals,
    ) {
        val headers = "HTTP/1.1 200 OK\r\n" +
                "Content-Type: text/event-stream\r\n" +
                "Connection: close\r\n" +
                "\r\n"
        output.write(headers.encodeToByteArray())
        output.flush()
        signals.responseHeadersWritten.complete(Unit)
        output.write(body)
        output.flush()
        signals.sseBodyWritten.complete(Unit)
    }

    private fun writeRawSseResponseHeaders(output: OutputStream) {
        val headers = "HTTP/1.1 200 OK\r\n" +
                "Content-Type: text/event-stream\r\n" +
                "Connection: close\r\n" +
                "\r\n"
        output.write(headers.encodeToByteArray())
        output.flush()
    }

    private fun Route.chatCompletionsProxyHandler(proxy: ChatCompletionsApiProxy) {
        post("/v1/chat/completions") {
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

    private fun Route.chatCompletionsAndOtherProxyHandler(proxy: ChatCompletionsApiProxy) {
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
                    return@handle call.respond(
                        OpenAiErrors.errorResponse(
                            e.message ?: "Unknown error",
                            "proxy_error",
                            status = HttpStatusCode.InternalServerError,
                        )
                    )
                }
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
    fun `proxy relays non-SSE upstream response from convert flow`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val upstreamBody = buildJsonObject {
            putJsonObject("error") {
                put("message", "bad request")
                put("type", "invalid_request_error")
            }
        }.toString()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/chat/completions") {
                    call.response.header("X-Upstream-Error", "bad-request")
                    call.respondText(upstreamBody, ContentType.Application.Json, HttpStatusCode.BadRequest)
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
                setBody(chatCompletionsRequest())
            }

            assertEquals(HttpStatusCode.BadRequest, response.status)
            assertEquals(ContentType.Application.Json, response.contentType()?.withoutParameters())
            assertEquals("bad-request", response.headers["X-Upstream-Error"])
            assertEquals(upstreamBody, response.bodyAsText())
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
    fun `passthrough when request is not JSON`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val capturedBody = AtomicReference<String>()
        val capturedContentType = AtomicReference<String>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/chat/completions") {
                    capturedBody.set(call.receiveText())
                    capturedContentType.set(call.request.headers[HttpHeaders.ContentType])
                    call.respondText("plain:${capturedBody.get()}", ContentType.Text.Plain)
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
                contentType(ContentType.Text.Plain)
                setBody("not json")
            }

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("not json", capturedBody.get())
            assertEquals(
                ContentType.Text.Plain,
                ContentType.parse(capturedContentType.get()).withoutParameters(),
            )
            assertEquals("plain:not json", response.bodyAsText())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `passthrough when JSON request has no model`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val requestBody = buildJsonObject {
            putJsonArray("messages") {
                add(buildJsonObject { put("role", "user") })
            }
        }.toString()
        val capturedBody = AtomicReference<String>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/chat/completions") {
                    capturedBody.set(call.receiveText())
                    call.respondText(
                        buildJsonObject { put("passthrough", true) }.toString(),
                        ContentType.Application.Json,
                    )
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
                setBody(requestBody)
            }

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals(requestBody, capturedBody.get())
            val body = Json.parseToJsonElement(response.bodyAsText()).jsonObject
            assertEquals(true, body["passthrough"]?.jsonPrimitive?.content?.toBoolean())
        } finally {
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `passthrough streams upstream response before upstream completes`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val upstreamSentFirst = CompletableDeferred<Unit>()
        val downstreamReceivedFirst = CompletableDeferred<Unit>()
        val upstreamSentSecond = CompletableDeferred<Unit>()
        val upstreamCanFinish = CompletableDeferred<Unit>()
        val events = Collections.synchronizedList(mutableListOf<String>())

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            install(ServerSSE)
            routing {
                route("/v1/chat/completions", HttpMethod.Post) {
                    sse {
                        send(data = "first")
                        events += "upstream-1"
                        upstreamSentFirst.complete(Unit)

                        downstreamReceivedFirst.await()

                        send(data = "second")
                        events += "upstream-2"
                        upstreamSentSecond.complete(Unit)
                        upstreamCanFinish.await()
                    }
                }
            }
        }.start()

        val proxy = ChatCompletionsApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { chatCompletionsProxyHandler(proxy) }
        }.start()

        val client = HttpClient(CIO.create()) {
            install(ClientSSE)
        }
        try {
            withTimeout(2.seconds) {
                client.sse({
                    url("http://127.0.0.1:$downstreamPort/v1/chat/completions")
                    method = HttpMethod.Post
                    contentType(ContentType.Application.Json)
                    setBody(chatCompletionsRequest(stream = true))
                }) {
                    var index = 0
                    incoming.take(2).collect { event ->
                        when (index++) {
                            0 -> {
                                assertEquals("first", event.data)
                                upstreamSentFirst.await()
                                events += "downstream-1"
                                downstreamReceivedFirst.complete(Unit)
                            }

                            1 -> {
                                assertEquals("second", event.data)
                                upstreamSentSecond.await()
                                events += "downstream-2"
                                upstreamCanFinish.complete(Unit)
                            }
                        }
                    }
                }
            }

            val observedEvents = synchronized(events) { events.toList() }
            assertEquals(
                listOf("upstream-1", "downstream-1", "upstream-2", "downstream-2"),
                observedEvents,
            )
        } finally {
            upstreamSentFirst.complete(Unit)
            downstreamReceivedFirst.complete(Unit)
            upstreamSentSecond.complete(Unit)
            upstreamCanFinish.complete(Unit)
            client.close()
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `passthrough returns 502 when upstream disconnects before response headers`() = runBlocking {
        withGatedPassthroughRawSseUpstream(writeResponseHeaders = false) { downstreamPort, signals ->
            val client = HttpClient(CIO.create()) {
                install(ClientSSE)
            }

            try {
                val downstreamFailure = async {
                    assertFailsWith<SSEClientException> {
                        client.sse({
                            url("http://127.0.0.1:$downstreamPort/v1/chat/completions")
                            method = HttpMethod.Post
                            contentType(ContentType.Application.Json)
                            setBody(chatCompletionsRequest(stream = true))
                        }) {
                            incoming.collect { }
                        }
                    }
                }

                withTimeout(2.seconds) {
                    signals.accepted.await()
                    signals.requestHeadersRead.await()
                }
                signals.disconnectRequested.complete(Unit)
                withTimeout(2.seconds) {
                    signals.disconnected.await()
                }

                val failure = withTimeout(2.seconds) { downstreamFailure.await() }
                val response = failure.response
                assertNotNull(response)
                assertEquals(HttpStatusCode.BadGateway, response.status)
                val body = Json.parseToJsonElement(response.bodyAsText()).jsonObject
                val error = body.getValue("error").jsonObject
                assertEquals("upstream_error", error.getValue("type").jsonPrimitive.content)
            } finally {
                client.close()
            }
        }
    }

    @Test
    fun `passthrough keeps started SSE response when upstream disconnects after response headers`() = runBlocking {
        val events = Collections.synchronizedList(mutableListOf<String>())

        withGatedPassthroughRawSseUpstream(
            writeResponseHeaders = true,
            events = events,
        ) { downstreamPort, signals ->
            val client = HttpClient(CIO.create()) {
                install(ClientSSE)
            }
            val downstreamSawResponse = CompletableDeferred<Unit>()

            try {
                val downstream = async {
                    client.sse({
                        url("http://127.0.0.1:$downstreamPort/v1/chat/completions")
                        method = HttpMethod.Post
                        contentType(ContentType.Application.Json)
                        setBody(chatCompletionsRequest(stream = true))
                    }) {
                        assertEquals(HttpStatusCode.OK, call.response.status)
                        assertEquals(ContentType.Text.EventStream, call.response.contentType()?.withoutParameters())
                        events += "downstream-start"
                        downstreamSawResponse.complete(Unit)
                        incoming.collect { event ->
                            events += "downstream-event:${event.data}"
                        }
                    }
                    events += "downstream-complete"
                }

                withTimeout(2.seconds) {
                    signals.responseHeadersWritten.await()
                    downstreamSawResponse.await()
                }
                signals.disconnectRequested.complete(Unit)
                withTimeout(2.seconds) {
                    signals.disconnected.await()
                    downstream.await()
                }

                val observedEvents = synchronized(events) { events.toList() }
                assertEquals(
                    listOf("upstream-headers", "downstream-start", "upstream-disconnect", "downstream-complete"),
                    observedEvents,
                )
            } finally {
                client.close()
            }
        }
    }

    @Test
    fun `passthrough keeps started SSE response when upstream disconnects after first event`() = runBlocking {
        val events = Collections.synchronizedList(mutableListOf<String>())

        withGatedPassthroughRawSseUpstream(
            writeResponseHeaders = true,
            writeFirstEvent = true,
            events = events,
        ) { downstreamPort, signals ->
            val client = HttpClient(CIO.create()) {
                install(ClientSSE)
            }
            val downstreamReceivedFirst = CompletableDeferred<Unit>()

            try {
                val downstream = async {
                    client.sse({
                        url("http://127.0.0.1:$downstreamPort/v1/chat/completions")
                        method = HttpMethod.Post
                        contentType(ContentType.Application.Json)
                        setBody(chatCompletionsRequest(stream = true))
                    }) {
                        assertEquals(HttpStatusCode.OK, call.response.status)
                        assertEquals(ContentType.Text.EventStream, call.response.contentType()?.withoutParameters())
                        incoming.collect { event ->
                            assertEquals("first", event.data)
                            events += "downstream-1"
                            downstreamReceivedFirst.complete(Unit)
                        }
                    }
                    events += "downstream-complete"
                }

                withTimeout(2.seconds) {
                    signals.firstEventWritten.await()
                    downstreamReceivedFirst.await()
                }
                signals.disconnectRequested.complete(Unit)
                withTimeout(2.seconds) {
                    signals.disconnected.await()
                    downstream.await()
                }

                val observedEvents = synchronized(events) { events.toList() }
                assertEquals(
                    listOf(
                        "upstream-headers",
                        "upstream-1",
                        "downstream-1",
                        "upstream-disconnect",
                        "downstream-complete",
                    ),
                    observedEvents,
                )
            } finally {
                client.close()
            }
        }
    }

    @Test
    fun `proxy returns 502 for incomplete SSE stream`() = runBlocking {
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
    fun `proxy returns 502 when upstream disconnects before SSE events`() = runBlocking {
        withDisconnectingSseUpstream(ByteArray(0)) { downstreamPort, signals ->
            val downstreamResponse = async {
                postChatCompletion(downstreamPort, chatCompletionsRequest())
            }
            withTimeout(2.seconds) {
                signals.accepted.await()
                signals.requestHeadersRead.await()
                signals.responseHeadersWritten.await()
                signals.disconnected.await()
            }
            val (status, body) = withTimeout(2.seconds) { downstreamResponse.await() }

            assertEquals(HttpStatusCode.BadGateway, status)
            val error = body.getValue("error").jsonObject
            assertEquals("upstream_error", error.getValue("type").jsonPrimitive.content)
        }
    }

    @Test
    fun `proxy returns 502 when upstream disconnects before terminal SSE event`() = runBlocking {
        val partialChunk = buildJsonObject {
            put("id", "chatcmpl-test")
            put("object", "chat.completion.chunk")
            putJsonArray("choices") {
                add(buildJsonObject {
                    putJsonObject("delta") { put("content", "Hi") }
                    put("index", 0)
                })
            }
        }
        val partialSseData = "data: $partialChunk\n\n".toByteArray()

        withDisconnectingSseUpstream(partialSseData) { downstreamPort, signals ->
            val downstreamResponse = async {
                postChatCompletion(downstreamPort, chatCompletionsRequest())
            }
            withTimeout(2.seconds) {
                signals.accepted.await()
                signals.requestHeadersRead.await()
                signals.responseHeadersWritten.await()
                signals.sseBodyWritten.await()
                signals.disconnected.await()
            }
            val (status, body) = withTimeout(2.seconds) { downstreamResponse.await() }

            assertEquals(HttpStatusCode.BadGateway, status)
            val error = body.getValue("error").jsonObject
            assertEquals("upstream_error", error.getValue("type").jsonPrimitive.content)
        }
    }

    @Test
    fun `proxy returns 502 when upstream is unreachable`() = runBlocking {
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
    fun `hop-by-hop response headers are not forwarded in passthrough flow`() = runBlocking {
        val body = "ok".encodeToByteArray()
        val rawResponse = (
                "HTTP/1.1 200 OK\r\n" +
                        "Content-Type: text/plain\r\n" +
                        "Content-Length: ${body.size}\r\n" +
                        "Connection: X-Remove-Me, close\r\n" +
                        "X-Remove-Me: should-not-forward\r\n" +
                        "Keep-Alive: timeout=5\r\n" +
                        "Upgrade: websocket\r\n" +
                        "X-Keep: yes\r\n" +
                        "\r\n"
                ).encodeToByteArray() + body

        withRawHttpUpstreamResponse(rawResponse) { downstreamPort ->
            val client = HttpClient(CIO.create())
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/other") {
                contentType(ContentType.Application.Json)
                setBody(buildJsonObject { put("model", "gpt-4") }.toString())
            }

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("yes", response.headers["X-Keep"])
            assertNull(response.headers["X-Remove-Me"])
            assertNull(response.headers["Keep-Alive"])
            assertNull(response.headers[HttpHeaders.Upgrade])
            assertEquals("ok", response.bodyAsText())
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
                    proxy.proxy(
                        requestMethod = call.request.httpMethod,
                        requestUri = call.request.uri,
                        requestHeaders = call.request.headers,
                        requestBody = call.receiveChannel(),
                        respond = { call.respond(it) },
                    )
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
