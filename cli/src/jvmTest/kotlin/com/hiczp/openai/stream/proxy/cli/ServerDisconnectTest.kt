package com.hiczp.openai.stream.proxy.cli

import io.ktor.client.engine.cio.*
import io.ktor.server.engine.*
import kotlinx.coroutines.*
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import io.ktor.server.cio.CIO as ServerCIO

class ServerDisconnectTest {
    private data class RawUpstreamSignals(
        val accepted: CompletableDeferred<Unit> = CompletableDeferred(),
        val requestRead: CompletableDeferred<Unit> = CompletableDeferred(),
        val responseHeadersWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val firstBodyWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val connectionClosed: CompletableDeferred<Unit> = CompletableDeferred(),
    ) {
        fun completeExceptionally(cause: Throwable) {
            accepted.completeExceptionally(cause)
            requestRead.completeExceptionally(cause)
            responseHeadersWritten.completeExceptionally(cause)
            firstBodyWritten.completeExceptionally(cause)
            connectionClosed.completeExceptionally(cause)
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

        val clientEngine = CIO.create()
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
            server.stop()
            clientEngine.close()
            acceptedSocket.get()?.close()
            upstreamSocket.close()
            if (!upstreamJob.isCompleted) {
                upstreamJob.cancelAndJoin()
            }
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

    private fun readUntilEof(input: InputStream) {
        try {
            while (input.read() != -1) {
                // Wait until the proxy closes its upstream socket.
            }
        } catch (_: SocketException) {
            // CIO may close the socket while the server side is blocked in read().
        }
    }

    private fun writeChunkedTextResponseHeaders(output: OutputStream) {
        output.write(
            (
                    "HTTP/1.1 200 OK\r\n" +
                            "Content-Type: text/plain\r\n" +
                            "Transfer-Encoding: chunked\r\n" +
                            "Connection: close\r\n" +
                            "\r\n"
                    ).encodeToByteArray()
        )
        output.flush()
    }

    private fun writeFirstChunk(output: OutputStream) {
        val bytes = "first".encodeToByteArray()
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
    ) = withContext(Dispatchers.IO) {
        val expected = needle.encodeToByteArray()
        var matched = 0
        while (matched < expected.size) {
            val byte = input.read()
            if (byte == -1) return@withContext
            matched = if (byte.toByte() == expected[matched]) {
                matched + 1
            } else if (byte.toByte() == expected[0]) {
                1
            } else {
                0
            }
        }
    }

    @Test
    fun `downstream disconnect during passthrough body cancels upstream response`() = runBlocking {
        withCliProxyToRawUpstream(
            upstreamHandler = { socket, signals ->
                val input = socket.getInputStream()
                val output = socket.getOutputStream()
                readHttpRequest(input)
                signals.requestRead.complete(Unit)
                writeChunkedTextResponseHeaders(output)
                signals.responseHeadersWritten.complete(Unit)
                writeFirstChunk(output)
                signals.firstBodyWritten.complete(Unit)
                readUntilEof(input)
                signals.connectionClosed.complete(Unit)
            },
        ) { downstreamPort, signals ->
            Socket("127.0.0.1", downstreamPort).use { downstreamSocket ->
                writeRawHttpRequest(downstreamSocket, "GET", "/v1/other")
                signals.accepted.await()
                signals.firstBodyWritten.await()
                readDownstreamUntil(downstreamSocket.getInputStream(), "first")

                downstreamSocket.close()

                signals.connectionClosed.await()
            }
        }
    }
}
