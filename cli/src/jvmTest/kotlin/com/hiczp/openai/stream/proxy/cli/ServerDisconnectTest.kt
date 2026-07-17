package com.hiczp.openai.stream.proxy.cli

import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.sse.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.server.engine.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.test.runTest
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertContains
import io.ktor.server.cio.CIO as ServerCIO

class ServerDisconnectTest {
    private enum class SocketCloseKind {
        EOF,
        RESET,
    }

    private enum class DownstreamCloseKind {
        FIN,
        RESET,
    }

    private enum class ConversionDisconnectPoint {
        AFTER_REQUEST,
        AFTER_RESPONSE_HEADERS,
        AFTER_FIRST_EVENT,
    }

    private enum class PassthroughDisconnectPoint {
        AFTER_REQUEST,
        AFTER_RESPONSE_HEADERS,
        AFTER_FIRST_BODY_CHUNK,
    }

    private data class RawUpstreamSignals(
        val accepted: CompletableDeferred<Unit> = CompletableDeferred(),
        val requestRead: CompletableDeferred<Unit> = CompletableDeferred(),
        val responseHeadersWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val firstBodyWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val upstreamResponseReceived: CompletableDeferred<Unit> = CompletableDeferred(),
        val upstreamSseCollectionStarted: CompletableDeferred<Unit> = CompletableDeferred(),
        val upstreamSseEventReceived: CompletableDeferred<Unit> = CompletableDeferred(),
        val connectionClosed: CompletableDeferred<SocketCloseKind> = CompletableDeferred(),
    ) {
        fun completeExceptionally(cause: Throwable) {
            accepted.completeExceptionally(cause)
            requestRead.completeExceptionally(cause)
            responseHeadersWritten.completeExceptionally(cause)
            firstBodyWritten.completeExceptionally(cause)
            upstreamResponseReceived.completeExceptionally(cause)
            upstreamSseCollectionStarted.completeExceptionally(cause)
            upstreamSseEventReceived.completeExceptionally(cause)
            connectionClosed.completeExceptionally(cause)
        }
    }

    @OptIn(io.ktor.utils.io.InternalAPI::class)
    private class ObservingHttpClientEngine(
        private val delegate: HttpClientEngine,
        private val signals: RawUpstreamSignals,
    ) : HttpClientEngine by delegate {
        override fun install(client: HttpClient) {
            super<HttpClientEngine>.install(client)
            client.responsePipeline.intercept(HttpResponsePipeline.After) { container ->
                val session = container.response as? ClientSSESession
                    ?: return@intercept
                val observedSession = ClientSSESession(
                    call = session.call,
                    delegate = object : SSESession by session {
                        override val incoming = session.incoming
                            .onStart { signals.upstreamSseCollectionStarted.complete(Unit) }
                            .onEach { signals.upstreamSseEventReceived.complete(Unit) }
                    },
                )
                proceedWith(HttpResponseContainer(container.expectedType, observedSession))
            }
        }

        override suspend fun execute(data: HttpRequestData): HttpResponseData {
            val response = delegate.execute(data)
            signals.upstreamResponseReceived.complete(Unit)
            return response
        }
    }

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }

    private suspend fun withCliProxyToRawUpstream(
        upstreamHandler: suspend CoroutineScope.(Socket, RawUpstreamSignals) -> Unit,
        block: suspend CoroutineScope.(downstreamPort: Int, signals: RawUpstreamSignals) -> Unit,
    ): Unit = coroutineScope {
        val upstreamSocket = ServerSocket(0)
        val upstreamPort = upstreamSocket.localPort
        val downstreamPort = findFreePort()
        val signals = RawUpstreamSignals()
        val acceptedSocket = AtomicReference<Socket>()
        val upstreamJob = async(Dispatchers.IO) {
            try {
                upstreamSocket.use { serverSocket ->
                    serverSocket.accept().use { socket ->
                        acceptedSocket.set(socket)
                        signals.accepted.complete(Unit)
                        upstreamHandler(socket, signals)
                    }
                }
            } catch (e: Throwable) {
                signals.completeExceptionally(e)
                throw e
            }
        }

        val clientEngine = ObservingHttpClientEngine(CIO.create(), signals)
        val server = embeddedServer(
            ServerCIO,
            configure = { connector { port = downstreamPort } }
        ) {
            configureProxyServer(
                clientEngine = clientEngine,
                rules = listOf(ProxyRule(downstreamPort, "http://127.0.0.1:$upstreamPort")),
                timeoutMillis = 600_000L,
            )
        }.start()

        try {
            block(downstreamPort, signals)
            upstreamJob.await()
        } finally {
            clientEngine.close()
            acceptedSocket.get()?.close()
            upstreamSocket.close()
            if (!upstreamJob.isCompleted) {
                upstreamJob.cancelAndJoin()
            }
            server.stop()
        }
    }

    private fun readHttpRequest(input: InputStream): Map<String, String> {
        val headerText = readHttpHeaders(input)
        val headers = headerText
            .lineSequence()
            .drop(1)
            .mapNotNull { line ->
                val separatorIndex = line.indexOf(':')
                if (separatorIndex == -1) {
                    null
                } else {
                    line.substring(0, separatorIndex).trim().lowercase() to
                            line.substring(separatorIndex + 1).trim()
                }
            }
            .toMap()

        headers["content-length"]?.toIntOrNull()?.takeIf { it > 0 }?.let { length ->
            readFully(input, length)
        }
        if (headers["transfer-encoding"]?.contains("chunked", ignoreCase = true) == true) {
            readChunkedBody(input)
        }

        return headers
    }

    private fun readHttpHeaders(input: InputStream): String {
        val delimiter = "\r\n\r\n".encodeToByteArray()
        val bytes = ByteArrayOutputStream()
        var matched = 0
        while (matched < delimiter.size) {
            val byte = input.read()
            if (byte == -1) break
            bytes.write(byte)
            matched = if (byte.toByte() == delimiter[matched]) {
                matched + 1
            } else if (byte.toByte() == delimiter[0]) {
                1
            } else {
                0
            }
        }
        return bytes.toByteArray().decodeToString()
    }

    private fun readFully(input: InputStream, length: Int) {
        var remaining = length
        val buffer = ByteArray(8192)
        while (remaining > 0) {
            val read = input.read(buffer, 0, minOf(buffer.size, remaining))
            if (read == -1) return
            remaining -= read
        }
    }

    private fun readChunkedBody(input: InputStream) {
        while (true) {
            val chunkSize = readAsciiLine(input)
                .substringBefore(';')
                .trim()
                .toInt(16)
            if (chunkSize == 0) {
                readAsciiLine(input)
                return
            }
            readFully(input, chunkSize)
            readAsciiLine(input)
        }
    }

    private fun readAsciiLine(input: InputStream): String {
        val bytes = ByteArrayOutputStream()
        var previous = -1
        while (true) {
            val current = input.read()
            if (current == -1) return bytes.toByteArray().decodeToString()
            if (previous == '\r'.code && current == '\n'.code) {
                val lineBytes = bytes.toByteArray()
                return lineBytes.copyOf(lineBytes.size - 1).decodeToString()
            }
            bytes.write(current)
            previous = current
        }
    }

    private fun readUntilClosed(input: InputStream): SocketCloseKind {
        try {
            while (input.read() != -1) {
                // Wait until the proxy closes its upstream socket.
            }
            return SocketCloseKind.EOF
        } catch (_: SocketException) {
            return SocketCloseKind.RESET
        }
    }

    private suspend fun awaitUpstreamClosed(signals: RawUpstreamSignals) {
        when (signals.connectionClosed.await()) {
            SocketCloseKind.EOF,
            SocketCloseKind.RESET -> Unit
        }
    }

    private fun writeChunkedResponseHeaders(output: OutputStream, contentType: String) {
        output.write(
            (
                    "HTTP/1.1 200 OK\r\n" +
                            "Content-Type: $contentType\r\n" +
                            "Transfer-Encoding: chunked\r\n" +
                            "Connection: close\r\n" +
                            "\r\n"
                    ).encodeToByteArray()
        )
        output.flush()
    }

    private fun writeChunk(output: OutputStream, content: String) {
        val bytes = content.encodeToByteArray()
        output.write(bytes.size.toString(16).encodeToByteArray())
        output.write("\r\n".encodeToByteArray())
        output.write(bytes)
        output.write("\r\n".encodeToByteArray())
        output.flush()
    }

    private fun writeRawHttpRequest(
        socket: Socket,
        method: String,
        path: String,
        body: String = "",
    ) {
        val bodyBytes = body.encodeToByteArray()
        val request = buildString {
            append("$method $path HTTP/1.1\r\n")
            append("Host: 127.0.0.1:${socket.port}\r\n")
            if (bodyBytes.isNotEmpty()) {
                append("Content-Type: application/json\r\n")
                append("Content-Length: ${bodyBytes.size}\r\n")
            }
            append("Connection: keep-alive\r\n")
            append("\r\n")
        }.encodeToByteArray()

        val output = socket.getOutputStream()
        output.write(request)
        output.write(bodyBytes)
        output.flush()
    }

    private suspend fun readDownstreamUntil(
        input: InputStream,
        needle: String,
    ) = runInterruptible(Dispatchers.IO) {
        val expected = needle.encodeToByteArray()
        var matched = 0
        while (matched < expected.size) {
            val byte = input.read()
            if (byte == -1) return@runInterruptible
            matched = if (byte.toByte() == expected[matched]) {
                matched + 1
            } else if (byte.toByte() == expected[0]) {
                1
            } else {
                0
            }
        }
    }

    private suspend fun assertDownstreamDisconnectDuringConversionCancelsUpstream(
        path: String,
        requestBody: String,
        firstSseEvent: String,
        disconnectPoint: ConversionDisconnectPoint,
        downstreamCloseKind: DownstreamCloseKind,
    ) {
        withCliProxyToRawUpstream(
            upstreamHandler = { socket, signals ->
                val input = socket.getInputStream()
                val output = socket.getOutputStream()
                readHttpRequest(input)
                signals.requestRead.complete(Unit)
                if (disconnectPoint != ConversionDisconnectPoint.AFTER_REQUEST) {
                    writeChunkedResponseHeaders(output, "text/event-stream")
                    signals.responseHeadersWritten.complete(Unit)
                }
                if (disconnectPoint == ConversionDisconnectPoint.AFTER_FIRST_EVENT) {
                    writeChunk(output, firstSseEvent)
                    signals.firstBodyWritten.complete(Unit)
                }
                signals.connectionClosed.complete(readUntilClosed(input))
            },
        ) { downstreamPort, signals ->
            Socket("127.0.0.1", downstreamPort).use { downstreamSocket ->
                writeRawHttpRequest(downstreamSocket, "POST", path, requestBody)
                signals.accepted.await()
                when (disconnectPoint) {
                    ConversionDisconnectPoint.AFTER_REQUEST -> signals.requestRead.await()
                    ConversionDisconnectPoint.AFTER_RESPONSE_HEADERS -> {
                        signals.responseHeadersWritten.await()
                        signals.upstreamResponseReceived.await()
                        signals.upstreamSseCollectionStarted.await()
                    }

                    ConversionDisconnectPoint.AFTER_FIRST_EVENT -> {
                        signals.firstBodyWritten.await()
                        signals.upstreamResponseReceived.await()
                        signals.upstreamSseEventReceived.await()
                    }
                }
                if (downstreamCloseKind == DownstreamCloseKind.RESET) {
                    downstreamSocket.setSoLinger(true, 0)
                }
            }

            awaitUpstreamClosed(signals)
        }
    }

    private suspend fun assertDownstreamDisconnectDuringPassthroughCancelsUpstream(
        disconnectPoint: PassthroughDisconnectPoint,
        downstreamCloseKind: DownstreamCloseKind,
    ) {
        withCliProxyToRawUpstream(
            upstreamHandler = { socket, signals ->
                val input = socket.getInputStream()
                val output = socket.getOutputStream()
                readHttpRequest(input)
                signals.requestRead.complete(Unit)
                if (disconnectPoint != PassthroughDisconnectPoint.AFTER_REQUEST) {
                    writeChunkedResponseHeaders(output, "text/plain")
                    signals.responseHeadersWritten.complete(Unit)
                }
                if (disconnectPoint == PassthroughDisconnectPoint.AFTER_FIRST_BODY_CHUNK) {
                    writeChunk(output, "first")
                    signals.firstBodyWritten.complete(Unit)
                }
                signals.connectionClosed.complete(readUntilClosed(input))
            },
        ) { downstreamPort, signals ->
            Socket("127.0.0.1", downstreamPort).use { downstreamSocket ->
                writeRawHttpRequest(downstreamSocket, "GET", "/v1/other")
                signals.accepted.await()
                when (disconnectPoint) {
                    PassthroughDisconnectPoint.AFTER_REQUEST -> signals.requestRead.await()
                    PassthroughDisconnectPoint.AFTER_RESPONSE_HEADERS -> {
                        signals.responseHeadersWritten.await()
                        readDownstreamUntil(downstreamSocket.getInputStream(), "\r\n\r\n")
                    }

                    PassthroughDisconnectPoint.AFTER_FIRST_BODY_CHUNK -> {
                        signals.firstBodyWritten.await()
                        readDownstreamUntil(downstreamSocket.getInputStream(), "first")
                    }
                }
                if (downstreamCloseKind == DownstreamCloseKind.RESET) {
                    downstreamSocket.setSoLinger(true, 0)
                }
            }

            awaitUpstreamClosed(signals)
        }
    }

    private suspend fun assertTerminalConversionEventClosesUpstream(
        path: String,
        requestBody: String,
        terminalSse: String,
        expectedResponseFragment: String,
    ) {
        withCliProxyToRawUpstream(
            upstreamHandler = { socket, signals ->
                val input = socket.getInputStream()
                val output = socket.getOutputStream()
                readHttpRequest(input)
                signals.requestRead.complete(Unit)
                writeChunkedResponseHeaders(output, "text/event-stream")
                signals.responseHeadersWritten.complete(Unit)
                writeChunk(output, terminalSse)
                signals.firstBodyWritten.complete(Unit)
                signals.connectionClosed.complete(readUntilClosed(input))
            },
        ) { downstreamPort, signals ->
            HttpClient(CIO).use { client ->
                val downstreamResponse = async {
                    client.post("http://127.0.0.1:$downstreamPort$path") {
                        contentType(ContentType.Application.Json)
                        setBody(requestBody)
                    }.bodyAsText()
                }

                signals.firstBodyWritten.await()
                awaitUpstreamClosed(signals)
                assertContains(downstreamResponse.await(), expectedResponseFragment)
            }
        }
    }

    @Test
    fun `downstream disconnect during passthrough cancels upstream at every response stage`() = runTest {
        PassthroughDisconnectPoint.entries.forEach { disconnectPoint ->
            DownstreamCloseKind.entries.forEach { downstreamCloseKind ->
                assertDownstreamDisconnectDuringPassthroughCancelsUpstream(
                    disconnectPoint = disconnectPoint,
                    downstreamCloseKind = downstreamCloseKind,
                )
            }
        }
    }

    @Test
    fun `downstream disconnect during responses conversion cancels upstream response`() = runTest {
        ConversionDisconnectPoint.entries.forEach { disconnectPoint ->
            DownstreamCloseKind.entries.forEach { downstreamCloseKind ->
                assertDownstreamDisconnectDuringConversionCancelsUpstream(
                    path = "/v1/responses",
                    requestBody = """{"model":"gpt-4.1","input":"hello"}""",
                    firstSseEvent = """
                        event: response.created
                        data: {"type":"response.created","response":{"id":"resp_1","status":"in_progress"}}
                    """.trimIndent() + "\n\n",
                    disconnectPoint = disconnectPoint,
                    downstreamCloseKind = downstreamCloseKind,
                )
            }
        }
    }

    @Test
    fun `downstream disconnect during chat completions conversion cancels upstream response`() = runTest {
        ConversionDisconnectPoint.entries.forEach { disconnectPoint ->
            DownstreamCloseKind.entries.forEach { downstreamCloseKind ->
                assertDownstreamDisconnectDuringConversionCancelsUpstream(
                    path = "/v1/chat/completions",
                    requestBody = """{"model":"gpt-4.1","messages":[{"role":"user","content":"hello"}]}""",
                    firstSseEvent = """
                        data: {"id":"chatcmpl_1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"partial"}}]}
                    """.trimIndent() + "\n\n",
                    disconnectPoint = disconnectPoint,
                    downstreamCloseKind = downstreamCloseKind,
                )
            }
        }
    }

    @Test
    fun `responses terminal event closes upstream before upstream EOF`() = runTest {
        assertTerminalConversionEventClosesUpstream(
            path = "/v1/responses",
            requestBody = """{"model":"gpt-4.1","input":"hello"}""",
            terminalSse = """
                event: response.completed
                data: {"type":"response.completed","response":{"id":"resp_done","object":"response","status":"completed","output":[]}}
            """.trimIndent() + "\n\n",
            expectedResponseFragment = "\"id\":\"resp_done\"",
        )
    }

    @Test
    fun `chat completions done event closes upstream before upstream EOF`() = runTest {
        assertTerminalConversionEventClosesUpstream(
            path = "/v1/chat/completions",
            requestBody = """{"model":"gpt-4.1","messages":[{"role":"user","content":"hello"}]}""",
            terminalSse = """
                data: {"id":"chatcmpl_done","object":"chat.completion.chunk","created":1,"model":"gpt-4.1","choices":[{"index":0,"delta":{"content":"done"},"finish_reason":"stop"}]}

                data: [DONE]
            """.trimIndent() + "\n\n",
            expectedResponseFragment = "\"id\":\"chatcmpl_done\"",
        )
    }
}
