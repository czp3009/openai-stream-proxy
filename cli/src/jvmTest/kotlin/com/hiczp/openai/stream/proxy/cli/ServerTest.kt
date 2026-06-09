package com.hiczp.openai.stream.proxy.cli

import com.hiczp.openai.stream.proxy.ChatCompletionsApiProxy
import com.hiczp.openai.stream.proxy.PassthroughApiProxy
import com.hiczp.openai.stream.proxy.ResponsesApiProxy
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.websocket.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.http.content.*
import io.ktor.http.websocket.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.websocket.*
import kotlinx.coroutines.*
import kotlinx.serialization.json.*
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets
import io.ktor.server.cio.CIO as ServerCIO
import io.ktor.server.websocket.WebSockets as ServerWebSockets
import io.ktor.server.websocket.webSocket as serverWebSocket

class ServerTest {
    private data class RawWebSocketFrame(
        val opcode: Int,
        val payload: ByteArray,
    )

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

    private fun readHttpHeaders(socket: Socket): String {
        val delimiter = "\r\n\r\n".encodeToByteArray()
        val bytes = mutableListOf<Byte>()
        var matched = 0
        while (matched < delimiter.size) {
            val byte = socket.getInputStream().read()
            if (byte == -1) break
            bytes += byte.toByte()
            matched = if (byte.toByte() == delimiter[matched]) matched + 1 else 0
        }
        return bytes.toByteArray().decodeToString()
    }

    private fun readRawWebSocketFrameOrNull(socket: Socket): RawWebSocketFrame? {
        val input = socket.getInputStream()
        val firstByte = input.read()
        if (firstByte == -1) return null

        val secondByte = input.read()
        if (secondByte == -1) return RawWebSocketFrame(firstByte and 0x0f, ByteArray(0))

        val masked = secondByte and 0x80 != 0
        val payloadLength = when (val length = secondByte and 0x7f) {
            126 -> input.readNBytes(2).fold(0) { acc, byte -> (acc shl 8) or (byte.toInt() and 0xff) }
            127 -> input.readNBytes(8).fold(0L) { acc, byte -> (acc shl 8) or (byte.toLong() and 0xff) }
                .also { require(it <= Int.MAX_VALUE) { "Frame payload too large for test: $it" } }
                .toInt()

            else -> length
        }
        val mask = if (masked) input.readNBytes(4) else null
        val payload = input.readNBytes(payloadLength)
        if (mask != null) {
            payload.forEachIndexed { index, byte ->
                payload[index] = (byte.toInt() xor mask[index % mask.size].toInt()).toByte()
            }
        }
        return RawWebSocketFrame(firstByte and 0x0f, payload)
    }

    private fun readRawWebSocketFrameOrNullWithTimeout(socket: Socket): RawWebSocketFrame? {
        val previousTimeout = socket.soTimeout
        socket.soTimeout = 5_000
        return try {
            readRawWebSocketFrameOrNull(socket)
        } catch (_: SocketTimeoutException) {
            error("Timed out waiting for downstream WebSocket close or TCP close")
        } finally {
            socket.soTimeout = previousTimeout
        }
    }

    private fun RawWebSocketFrame.closeCode(): Short {
        require(payload.size >= 2) { "Close frame payload does not contain a close code" }
        return (((payload[0].toInt() and 0xff) shl 8) or (payload[1].toInt() and 0xff)).toShort()
    }

    private fun RawWebSocketFrame.closeMessage(): String = payload.copyOfRange(2, payload.size).decodeToString()

    private fun Socket.writeWebSocketHandshake(port: Int) {
        val request = buildString {
            append("GET /v1/realtime HTTP/1.1\r\n")
            append("Host: 127.0.0.1:$port\r\n")
            append("Upgrade: websocket\r\n")
            append("Connection: Upgrade\r\n")
            append("Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n")
            append("Sec-WebSocket-Version: 13\r\n")
            append("\r\n")
        }
        getOutputStream().write(request.encodeToByteArray())
        getOutputStream().flush()
    }

    private fun Socket.writeMaskedTextFrame(text: String) {
        val payload = text.encodeToByteArray()
        val mask = byteArrayOf(1, 2, 3, 4)
        val frame = ByteArray(2 + mask.size + payload.size)
        frame[0] = 0x81.toByte()
        frame[1] = (0x80 or payload.size).toByte()
        mask.copyInto(frame, destinationOffset = 2)
        payload.forEachIndexed { index, byte ->
            frame[2 + mask.size + index] = (byte.toInt() xor mask[index % mask.size].toInt()).toByte()
        }
        getOutputStream().write(frame)
        getOutputStream().flush()
    }

    private fun Socket.writeUnmaskedTextFrame(text: String) {
        val payload = text.encodeToByteArray()
        require(payload.size < 126) { "Test helper only supports small frames" }
        getOutputStream().write(byteArrayOf(0x81.toByte(), payload.size.toByte()))
        getOutputStream().write(payload)
        getOutputStream().flush()
    }

    private fun Socket.writeUnmaskedCloseFrame(reason: CloseReason) {
        val messageBytes = reason.message.encodeToByteArray()
        val payload = ByteArray(2 + messageBytes.size)
        payload[0] = ((reason.code.toInt() shr 8) and 0xff).toByte()
        payload[1] = (reason.code.toInt() and 0xff).toByte()
        messageBytes.copyInto(payload, destinationOffset = 2)
        require(payload.size < 126) { "Test helper only supports small close frames" }
        getOutputStream().write(byteArrayOf(0x88.toByte(), payload.size.toByte()))
        getOutputStream().write(payload)
        getOutputStream().flush()
    }

    private fun Socket.writePartialUnmaskedTextFrame(declaredPayloadLength: Int, payloadPrefix: String) {
        val payload = payloadPrefix.encodeToByteArray()
        require(declaredPayloadLength in 0..125) { "Test helper only supports small frames" }
        require(payload.size < declaredPayloadLength) { "Payload prefix must be shorter than declared payload length" }
        getOutputStream().write(byteArrayOf(0x81.toByte(), declaredPayloadLength.toByte()))
        getOutputStream().write(payload)
        getOutputStream().flush()
    }

    private fun writeRawWebSocketHandshakeResponse(socket: Socket, requestHeaders: String) {
        val key = requestHeaders
            .lineSequence()
            .first { it.startsWith("${HttpHeaders.SecWebSocketKey}:", ignoreCase = true) }
            .substringAfter(':')
            .trim()
        val response = buildString {
            append("HTTP/1.1 101 Switching Protocols\r\n")
            append("Upgrade: websocket\r\n")
            append("Connection: Upgrade\r\n")
            append("Sec-WebSocket-Accept: ${websocketServerAccept(key)}\r\n")
            append("\r\n")
        }
        socket.getOutputStream().write(response.encodeToByteArray())
        socket.getOutputStream().flush()
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
    fun `configureProxyServer forwards websocket frames to upstream`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val capturedUri = CompletableDeferred<String>()
        val capturedHost = CompletableDeferred<String?>()
        val capturedAuthorization = CompletableDeferred<String?>()
        val capturedOpenAiBeta = CompletableDeferred<String?>()
        val capturedProtocol = CompletableDeferred<String?>()
        val capturedRemoveMe = CompletableDeferred<String?>()
        val capturedText = CompletableDeferred<String>()
        val capturedBinary = CompletableDeferred<ByteArray>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            install(ServerWebSockets)
            routing {
                serverWebSocket("/{...}") {
                    capturedUri.complete(call.request.uri)
                    capturedHost.complete(call.request.headers[HttpHeaders.Host])
                    capturedAuthorization.complete(call.request.headers[HttpHeaders.Authorization])
                    capturedOpenAiBeta.complete(call.request.headers["OpenAI-Beta"])
                    capturedProtocol.complete(call.request.headers[HttpHeaders.SecWebSocketProtocol])
                    capturedRemoveMe.complete(call.request.headers["X-Remove-Me"])

                    val textFrame = incoming.receive() as Frame.Text
                    capturedText.complete(textFrame.readText())
                    send(Frame.Text(true, textFrame.data))

                    val binaryFrame = incoming.receive() as Frame.Binary
                    capturedBinary.complete(binaryFrame.data)
                    send(Frame.Binary(true, binaryFrame.data))
                }
            }
        }.start()
        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            configureProxyServer(
                CIO.create(),
                listOf(ProxyRule(downstreamPort, "http://127.0.0.1:$upstreamPort/upstream-base")),
                600_000L,
            )
        }.start()

        val client = HttpClient(CIO) {
            install(ClientWebSockets)
        }
        val binaryPayload = byteArrayOf(1, 2, 3, 4)
        try {
            client.webSocket(
                urlString = "ws://127.0.0.1:$downstreamPort/v1/realtime?model=gpt-realtime",
                request = {
                    header(HttpHeaders.Connection, "Upgrade, X-Remove-Me")
                    header(HttpHeaders.Authorization, "Bearer ws-test")
                    header("OpenAI-Beta", "realtime=v1")
                    header(HttpHeaders.SecWebSocketProtocol, "realtime")
                    header("X-Remove-Me", "remove")
                },
            ) {
                send(Frame.Text("hello"))
                val echoedText = incoming.receive() as Frame.Text
                assertEquals("hello", echoedText.readText())

                send(Frame.Binary(true, binaryPayload))
                val echoedBinary = incoming.receive() as Frame.Binary
                assertTrue(binaryPayload.contentEquals(echoedBinary.data))
            }

            assertEquals("/upstream-base/v1/realtime?model=gpt-realtime", capturedUri.await())
            assertEquals("127.0.0.1:$upstreamPort", capturedHost.await())
            assertEquals("Bearer ws-test", capturedAuthorization.await())
            assertEquals("realtime=v1", capturedOpenAiBeta.await())
            assertEquals(null, capturedProtocol.await())
            assertEquals(null, capturedRemoveMe.await())
            assertEquals("hello", capturedText.await())
            assertTrue(binaryPayload.contentEquals(capturedBinary.await()))
        } finally {
            client.close()
            downstreamServer.stop()
            upstreamServer.stop()
        }
    }

    @Test
    fun `configureProxyServer closes upstream websocket when downstream disconnects`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val upstreamReceived = CompletableDeferred<String>()
        val upstreamClosed = CompletableDeferred<Unit>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            install(ServerWebSockets)
            routing {
                serverWebSocket("/{...}") {
                    try {
                        upstreamReceived.complete((incoming.receive() as Frame.Text).readText())
                        for (frame in incoming) {
                            if (frame is Frame.Close) break
                        }
                    } finally {
                        upstreamClosed.complete(Unit)
                    }
                }
            }
        }.start()
        val clientEngine = CIO.create()
        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            configureProxyServer(
                clientEngine,
                listOf(ProxyRule(downstreamPort, "http://127.0.0.1:$upstreamPort")),
                600_000L,
            )
        }.start()

        try {
            Socket("127.0.0.1", downstreamPort).use { socket ->
                socket.writeWebSocketHandshake(downstreamPort)
                assertTrue(readHttpHeaders(socket).startsWith("HTTP/1.1 101"))
                socket.writeMaskedTextFrame("disconnect")
                assertEquals("disconnect", upstreamReceived.await())
            }

            withTimeout(5_000) {
                upstreamClosed.await()
            }
        } finally {
            downstreamServer.stop()
            clientEngine.close()
            upstreamServer.stop()
        }
    }

    @Test
    fun `installErrorHandler sees websocket calls as responded after websocket upgrade`() = runBlocking {
        val downstreamPort = findFreePort()
        val responseState = CompletableDeferred<Pair<Boolean, Boolean>>()

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            install(ServerWebSockets)
            installErrorHandler()
            routing {
                serverWebSocket("/{...}") {
                    responseState.complete(call.response.isCommitted to call.response.isSent)
                    error("websocket handler failure")
                }
            }
        }.start()

        try {
            Socket("127.0.0.1", downstreamPort).use { socket ->
                socket.writeWebSocketHandshake(downstreamPort)
                assertTrue(readHttpHeaders(socket).startsWith("HTTP/1.1 101"))
                assertEquals(true to false, responseState.await())
                assertNull(readRawWebSocketFrameOrNullWithTimeout(socket))
            }
        } finally {
            downstreamServer.stop()
        }
    }

    @Test
    fun `configureProxyServer accepts downstream websocket before upstream rejects websocket handshake`() =
        runBlocking {
            listOf(
                HttpStatusCode.NotFound,
                HttpStatusCode.MethodNotAllowed,
                HttpStatusCode.NotImplemented,
            ).forEach { upstreamStatus ->
                val upstreamPort = findFreePort()
                val downstreamPort = findFreePort()
                val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
                    routing {
                        get("/{...}") {
                            call.respondText("upstream ${upstreamStatus.value}", status = upstreamStatus)
                        }
                    }
                }.start()
                val clientEngine = CIO.create()
                val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
                    configureProxyServer(
                        clientEngine,
                        listOf(ProxyRule(downstreamPort, "http://127.0.0.1:$upstreamPort")),
                        600_000L,
                    )
                }.start()

                try {
                    Socket("127.0.0.1", downstreamPort).use { socket ->
                        socket.writeWebSocketHandshake(downstreamPort)
                        val downstreamHandshake = readHttpHeaders(socket)
                        assertTrue(
                            downstreamHandshake.startsWith("HTTP/1.1 101"),
                            "Downstream should already receive 101 for upstream ${upstreamStatus.value}: $downstreamHandshake",
                        )
                        val closeFrame = readRawWebSocketFrameOrNullWithTimeout(socket)
                            ?: error("Proxy should send a WebSocket close frame for upstream ${upstreamStatus.value}")
                        assertEquals(8, closeFrame.opcode)
                        assertEquals(CloseReason.Codes.INTERNAL_ERROR.code, closeFrame.closeCode())
                        assertEquals(
                            "Handshake exception, expected status code 101 but was ${upstreamStatus.value}",
                            closeFrame.closeMessage(),
                        )
                    }
                } finally {
                    downstreamServer.stop()
                    clientEngine.close()
                    upstreamServer.stop()
                }
            }
        }

    @Test
    fun `configureProxyServer closes downstream websocket when upstream websocket connection fails`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val clientEngine = CIO.create()
        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            configureProxyServer(
                clientEngine,
                listOf(ProxyRule(downstreamPort, "http://127.0.0.1:$upstreamPort")),
                600_000L,
            )
        }.start()

        try {
            Socket("127.0.0.1", downstreamPort).use { socket ->
                socket.writeWebSocketHandshake(downstreamPort)
                assertTrue(readHttpHeaders(socket).startsWith("HTTP/1.1 101"))
                assertNull(readRawWebSocketFrameOrNullWithTimeout(socket))
            }
        } finally {
            downstreamServer.stop()
            clientEngine.close()
        }
    }

    @Test
    fun `configureProxyServer forwards upstream websocket close reason`() =
        runBlocking<Unit> {
            val upstreamSocket = ServerSocket(0)
            val upstreamPort = upstreamSocket.localPort
            val downstreamPort = findFreePort()
            val upstreamJob = async(Dispatchers.IO) {
                upstreamSocket.use { serverSocket ->
                    serverSocket.accept().use { socket ->
                        writeRawWebSocketHandshakeResponse(socket, readHttpHeaders(socket))
                        socket.writeUnmaskedCloseFrame(CloseReason(CloseReason.Codes.NORMAL, "done"))
                        readRawWebSocketFrameOrNullWithTimeout(socket)
                    }
                }
            }
            val clientEngine = CIO.create()
            val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
                configureProxyServer(
                    clientEngine,
                    listOf(ProxyRule(downstreamPort, "http://127.0.0.1:$upstreamPort")),
                    600_000L,
                )
            }.start()

            try {
                Socket("127.0.0.1", downstreamPort).use { socket ->
                    socket.writeWebSocketHandshake(downstreamPort)
                    assertTrue(readHttpHeaders(socket).startsWith("HTTP/1.1 101"))

                    val closeFrame = readRawWebSocketFrameOrNullWithTimeout(socket)
                        ?: error("Proxy should forward upstream WebSocket close frame")
                    assertEquals(8, closeFrame.opcode)
                    assertEquals(CloseReason.Codes.NORMAL.code, closeFrame.closeCode())
                    assertEquals("done", closeFrame.closeMessage())
                }
                upstreamJob.await()
            } finally {
                downstreamServer.stop()
                clientEngine.close()
                upstreamSocket.close()
                if (!upstreamJob.isCompleted) {
                    upstreamJob.cancelAndJoin()
                }
            }
        }

    @Test
    fun `configureProxyServer reports abnormal close when upstream disconnects after websocket handshake`() =
        runBlocking<Unit> {
            val upstreamSocket = ServerSocket(0)
            val upstreamPort = upstreamSocket.localPort
            val downstreamPort = findFreePort()
            val upstreamHandshakeCompleted = CompletableDeferred<Unit>()
            val upstreamJob = async(Dispatchers.IO) {
                upstreamSocket.use { serverSocket ->
                    serverSocket.accept().use { socket ->
                        writeRawWebSocketHandshakeResponse(socket, readHttpHeaders(socket))
                        upstreamHandshakeCompleted.complete(Unit)
                    }
                }
            }
            val clientEngine = CIO.create()
            val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
                configureProxyServer(
                    clientEngine,
                    listOf(ProxyRule(downstreamPort, "http://127.0.0.1:$upstreamPort")),
                    600_000L,
                )
            }.start()

            try {
                Socket("127.0.0.1", downstreamPort).use { socket ->
                    socket.writeWebSocketHandshake(downstreamPort)
                    assertTrue(readHttpHeaders(socket).startsWith("HTTP/1.1 101"))
                    upstreamHandshakeCompleted.await()

                    val closeFrame = readRawWebSocketFrameOrNullWithTimeout(socket)
                        ?: error("Proxy should send a WebSocket close frame after upstream disconnects")
                    assertEquals(8, closeFrame.opcode)
                    assertEquals(CloseReason.Codes.INTERNAL_ERROR.code, closeFrame.closeCode())
                    assertEquals("upstream websocket disconnected abnormally", closeFrame.closeMessage())
                }
                upstreamJob.await()
            } finally {
                downstreamServer.stop()
                clientEngine.close()
                upstreamSocket.close()
                if (!upstreamJob.isCompleted) {
                    upstreamJob.cancelAndJoin()
                }
            }
        }

    @Test
    fun `configureProxyServer forwards upstream frame before upstream disconnects without close frame`() =
        runBlocking<Unit> {
            val upstreamSocket = ServerSocket(0)
            val upstreamPort = upstreamSocket.localPort
            val downstreamPort = findFreePort()
            val upstreamJob = async(Dispatchers.IO) {
                upstreamSocket.use { serverSocket ->
                    serverSocket.accept().use { socket ->
                        writeRawWebSocketHandshakeResponse(socket, readHttpHeaders(socket))
                        socket.writeUnmaskedTextFrame("before disconnect")
                    }
                }
            }
            val clientEngine = CIO.create()
            val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
                configureProxyServer(
                    clientEngine,
                    listOf(ProxyRule(downstreamPort, "http://127.0.0.1:$upstreamPort")),
                    600_000L,
                )
            }.start()

            try {
                Socket("127.0.0.1", downstreamPort).use { socket ->
                    socket.writeWebSocketHandshake(downstreamPort)
                    assertTrue(readHttpHeaders(socket).startsWith("HTTP/1.1 101"))

                    val textFrame = readRawWebSocketFrameOrNullWithTimeout(socket)
                        ?: error("Proxy should forward the complete upstream frame")
                    assertEquals(1, textFrame.opcode)
                    assertEquals("before disconnect", textFrame.payload.decodeToString())

                    val closeFrame = readRawWebSocketFrameOrNullWithTimeout(socket)
                        ?: error("Proxy should send a WebSocket close frame after upstream disconnects")
                    assertEquals(8, closeFrame.opcode)
                    assertEquals(CloseReason.Codes.INTERNAL_ERROR.code, closeFrame.closeCode())
                    assertEquals("upstream websocket disconnected abnormally", closeFrame.closeMessage())
                }
                upstreamJob.await()
            } finally {
                downstreamServer.stop()
                clientEngine.close()
                upstreamSocket.close()
                if (!upstreamJob.isCompleted) {
                    upstreamJob.cancelAndJoin()
                }
            }
        }

    @Test
    fun `configureProxyServer does not forward incomplete upstream frame before upstream disconnects`() =
        runBlocking<Unit> {
            val upstreamSocket = ServerSocket(0)
            val upstreamPort = upstreamSocket.localPort
            val downstreamPort = findFreePort()
            val upstreamJob = async(Dispatchers.IO) {
                upstreamSocket.use { serverSocket ->
                    serverSocket.accept().use { socket ->
                        writeRawWebSocketHandshakeResponse(socket, readHttpHeaders(socket))
                        socket.writePartialUnmaskedTextFrame(
                            declaredPayloadLength = "incomplete message".encodeToByteArray().size,
                            payloadPrefix = "incomplete",
                        )
                    }
                }
            }
            val clientEngine = CIO.create()
            val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
                configureProxyServer(
                    clientEngine,
                    listOf(ProxyRule(downstreamPort, "http://127.0.0.1:$upstreamPort")),
                    600_000L,
                )
            }.start()

            try {
                Socket("127.0.0.1", downstreamPort).use { socket ->
                    socket.writeWebSocketHandshake(downstreamPort)
                    assertTrue(readHttpHeaders(socket).startsWith("HTTP/1.1 101"))

                    val closeFrame = readRawWebSocketFrameOrNullWithTimeout(socket)
                        ?: error("Proxy should send a WebSocket close frame after incomplete upstream frame")
                    assertEquals(8, closeFrame.opcode)
                    assertEquals(CloseReason.Codes.INTERNAL_ERROR.code, closeFrame.closeCode())
                    assertEquals("upstream websocket disconnected abnormally", closeFrame.closeMessage())
                }
                upstreamJob.await()
            } finally {
                downstreamServer.stop()
                clientEngine.close()
                upstreamSocket.close()
                if (!upstreamJob.isCompleted) {
                    upstreamJob.cancelAndJoin()
                }
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
