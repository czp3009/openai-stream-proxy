package com.hiczp.openai.stream.proxy

import io.ktor.client.engine.cio.*
import io.ktor.http.*
import io.ktor.server.engine.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.coroutines.*
import kotlinx.serialization.json.*
import java.io.ByteArrayOutputStream
import java.io.EOFException
import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import kotlin.test.Test
import kotlin.test.assertEquals
import io.ktor.server.cio.CIO as ServerCIO

class PassthroughDisconnectTest {
    private enum class RawUpstreamFailure {
        CloseAfterAccept,
        CloseAfterRequest,
        CloseAfterStatusOnlyResponse,
        CloseAfterResponseHeaders,
        CloseAfterPartialBody,
    }

    private data class RawUpstreamSignals(
        val accepted: CompletableDeferred<Unit> = CompletableDeferred(),
        val requestRead: CompletableDeferred<Unit> = CompletableDeferred(),
        val statusOnlyResponseWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val responseHeadersWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val partialBodyWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val disconnectRequested: CompletableDeferred<Unit> = CompletableDeferred(),
        val disconnected: CompletableDeferred<Unit> = CompletableDeferred(),
    ) {
        fun completeExceptionally(cause: Throwable) {
            accepted.completeExceptionally(cause)
            requestRead.completeExceptionally(cause)
            statusOnlyResponseWritten.completeExceptionally(cause)
            responseHeadersWritten.completeExceptionally(cause)
            partialBodyWritten.completeExceptionally(cause)
            disconnectRequested.completeExceptionally(cause)
            disconnected.completeExceptionally(cause)
        }
    }

    private data class RawDownstreamSignals(
        val requestSent: CompletableDeferred<Unit> = CompletableDeferred(),
        val responseHeadersRead: CompletableDeferred<Unit> = CompletableDeferred(),
        val expectedBodyFragmentRead: CompletableDeferred<Unit> = CompletableDeferred(),
        val responseBodyRead: CompletableDeferred<Unit> = CompletableDeferred(),
    ) {
        fun completeExceptionally(cause: Throwable) {
            requestSent.completeExceptionally(cause)
            responseHeadersRead.completeExceptionally(cause)
            expectedBodyFragmentRead.completeExceptionally(cause)
            responseBodyRead.completeExceptionally(cause)
        }
    }

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }

    private suspend fun withFailingRawUpstream(
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
                            readHttpRequest(socket.getInputStream())
                            signals.requestRead.complete(Unit)
                        }

                        val output = socket.getOutputStream()
                        when (failure) {
                            RawUpstreamFailure.CloseAfterAccept,
                            RawUpstreamFailure.CloseAfterRequest -> signals.disconnectRequested.await()

                            RawUpstreamFailure.CloseAfterStatusOnlyResponse -> {
                                output.write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".encodeToByteArray())
                                output.flush()
                                signals.statusOnlyResponseWritten.complete(Unit)
                                signals.disconnectRequested.await()
                            }

                            RawUpstreamFailure.CloseAfterResponseHeaders -> {
                                writePlainResponseHeaders(output)
                                signals.responseHeadersWritten.complete(Unit)
                                signals.disconnectRequested.await()
                            }

                            RawUpstreamFailure.CloseAfterPartialBody -> {
                                writePlainResponseHeaders(output)
                                signals.responseHeadersWritten.complete(Unit)
                                output.write("partial".encodeToByteArray())
                                output.flush()
                                signals.partialBodyWritten.complete(Unit)
                                signals.disconnectRequested.await()
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
        val proxy = ChatCompletionsApiProxy(engine, "http://127.0.0.1:$upstreamPort")
        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            routing { proxyHandler(proxy) }
        }.start()

        try {
            block(downstreamPort, signals)
            signals.disconnectRequested.complete(Unit)
            upstreamJob.await()
        } finally {
            signals.disconnectRequested.complete(Unit)
            downstreamServer.stop()
            engine.close()
            upstreamSocket.close()
            if (!upstreamJob.isCompleted) {
                upstreamJob.cancelAndJoin()
            }
        }
    }

    private suspend fun postPassthroughRaw(
        downstreamPort: Int,
        expectedBodyFragment: String? = null,
        signals: RawDownstreamSignals = RawDownstreamSignals(),
    ): Pair<HttpStatusCode, String> = withContext(Dispatchers.IO) {
        try {
            Socket("127.0.0.1", downstreamPort).use { socket ->
                socket.getOutputStream().writeRawPassthroughRequest()
                signals.requestSent.complete(Unit)
                val input = socket.getInputStream()
                val responseHeaderText = readHttpHeaders(input)
                val status = parseStatus(responseHeaderText)
                val headers = parseHeaderMap(responseHeaderText)
                signals.responseHeadersRead.complete(Unit)
                val body = readHttpBody(input, headers, expectedBodyFragment, signals)
                signals.responseBodyRead.complete(Unit)
                status to body
            }
        } catch (e: Throwable) {
            signals.completeExceptionally(e)
            throw e
        }
    }

    private fun Route.proxyHandler(proxy: ChatCompletionsApiProxy) {
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

    private fun OutputStream.writeRawPassthroughRequest() {
        val body = buildJsonObject { put("model", "gpt-4") }.toString().encodeToByteArray()
        write(
            (
                    "POST /v1/other HTTP/1.1\r\n" +
                            "Host: 127.0.0.1\r\n" +
                            "Content-Type: application/json\r\n" +
                            "Content-Length: ${body.size}\r\n" +
                            "Connection: close\r\n" +
                            "\r\n"
                    ).encodeToByteArray()
        )
        write(body)
        flush()
    }

    private fun readHttpRequest(input: InputStream) {
        val headerText = readHttpHeaders(input)
        val contentLength = headerText
            .lineSequence()
            .firstOrNull { it.startsWith("Content-Length:", ignoreCase = true) }
            ?.substringAfter(':')
            ?.trim()
            ?.toIntOrNull()
            ?: 0

        var remaining = contentLength
        val buffer = ByteArray(8192)
        while (remaining > 0) {
            val read = input.read(buffer, 0, minOf(buffer.size, remaining))
            if (read == -1) return
            remaining -= read
        }
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

    private fun parseStatus(responseHeaderText: String): HttpStatusCode {
        val statusCode = responseHeaderText
            .lineSequence()
            .first()
            .split(' ', limit = 3)
            .getOrNull(1)
            ?.toInt()
            ?: error("Invalid HTTP response status line: $responseHeaderText")
        return HttpStatusCode.fromValue(statusCode)
    }

    private fun parseHeaderMap(responseHeaderText: String): Map<String, List<String>> =
        responseHeaderText
            .lineSequence()
            .drop(1)
            .map { it.trimEnd('\r') }
            .filter { it.isNotBlank() }
            .mapNotNull { line ->
                val separator = line.indexOf(':')
                if (separator < 0) {
                    null
                } else {
                    line.substring(0, separator).lowercase() to line.substring(separator + 1).trim()
                }
            }
            .groupBy({ it.first }, { it.second })

    private fun readHttpBody(
        input: InputStream,
        headers: Map<String, List<String>>,
        expectedBodyFragment: String?,
        signals: RawDownstreamSignals,
    ): String {
        val body = ByteArrayOutputStream()
        val transferEncoding = headers["transfer-encoding"].orEmpty().joinToString(",").lowercase()
        val contentLength = headers["content-length"]?.lastOrNull()?.toIntOrNull()

        when {
            "chunked" in transferEncoding -> readChunkedBody(input, body, expectedBodyFragment, signals)
            contentLength != null -> readFixedLengthBody(input, body, contentLength, expectedBodyFragment, signals)
            else -> readCloseDelimitedBody(input, body, expectedBodyFragment, signals)
        }

        return body.toByteArray().decodeToString()
    }

    private fun readFixedLengthBody(
        input: InputStream,
        body: ByteArrayOutputStream,
        contentLength: Int,
        expectedBodyFragment: String?,
        signals: RawDownstreamSignals,
    ) {
        var remaining = contentLength
        val buffer = ByteArray(8192)
        while (remaining > 0) {
            val read = input.read(buffer, 0, minOf(buffer.size, remaining))
            if (read == -1) throw EOFException("Unexpected EOF while reading fixed-length response body")
            body.write(buffer, 0, read)
            remaining -= read
            completeIfBodyContains(body, expectedBodyFragment, signals)
        }
    }

    private fun readCloseDelimitedBody(
        input: InputStream,
        body: ByteArrayOutputStream,
        expectedBodyFragment: String?,
        signals: RawDownstreamSignals,
    ) {
        val buffer = ByteArray(8192)
        while (true) {
            val read = input.read(buffer)
            if (read == -1) return
            body.write(buffer, 0, read)
            completeIfBodyContains(body, expectedBodyFragment, signals)
        }
    }

    private fun readChunkedBody(
        input: InputStream,
        body: ByteArrayOutputStream,
        expectedBodyFragment: String?,
        signals: RawDownstreamSignals,
    ) {
        while (true) {
            val sizeLine = try {
                readHttpLine(input)
            } catch (_: EOFException) {
                return
            }
            val chunkSize = sizeLine.substringBefore(';').trim().toInt(16)
            if (chunkSize == 0) {
                while (readHttpLine(input).isNotEmpty()) {
                    // Consume trailing headers.
                }
                return
            }

            val chunk = readExactly(input, chunkSize)
            body.write(chunk)
            completeIfBodyContains(body, expectedBodyFragment, signals)
            readHttpLine(input)
        }
    }

    private fun readExactly(input: InputStream, size: Int): ByteArray {
        val bytes = ByteArray(size)
        var offset = 0
        while (offset < size) {
            val read = input.read(bytes, offset, size - offset)
            if (read == -1) throw EOFException("Unexpected EOF while reading $size bytes")
            offset += read
        }
        return bytes
    }

    private fun readHttpLine(input: InputStream): String {
        val bytes = ByteArrayOutputStream()
        while (true) {
            val byte = input.read()
            if (byte == -1) {
                if (bytes.size() == 0) throw EOFException("Unexpected EOF while reading line")
                return bytes.toByteArray().decodeToString()
            }
            if (byte == '\r'.code) {
                val next = input.read()
                if (next == '\n'.code) return bytes.toByteArray().decodeToString()
                bytes.write(byte)
                if (next == -1) return bytes.toByteArray().decodeToString()
                bytes.write(next)
            } else {
                bytes.write(byte)
            }
        }
    }

    private fun completeIfBodyContains(
        body: ByteArrayOutputStream,
        expectedBodyFragment: String?,
        signals: RawDownstreamSignals,
    ) {
        if (expectedBodyFragment != null && body.toByteArray().decodeToString().contains(expectedBodyFragment)) {
            signals.expectedBodyFragmentRead.complete(Unit)
        }
    }

    private fun writePlainResponseHeaders(output: OutputStream) {
        output.write(
            (
                    "HTTP/1.1 200 OK\r\n" +
                            "Content-Type: text/plain\r\n" +
                            "Connection: close\r\n" +
                            "\r\n"
                    ).encodeToByteArray()
        )
        output.flush()
    }

    @Test
    fun `passthrough returns 504 when upstream IO fails before usable response`() = runBlocking {
        val failures = listOf(
            RawUpstreamFailure.CloseAfterAccept,
            RawUpstreamFailure.CloseAfterRequest,
        )

        failures.forEach { failure ->
            withFailingRawUpstream(failure) { downstreamPort, signals ->
                val downstreamSignals = RawDownstreamSignals()
                val downstreamResponse = async { postPassthroughRaw(downstreamPort, signals = downstreamSignals) }

                downstreamSignals.requestSent.await()
                signals.accepted.await()
                when (failure) {
                    RawUpstreamFailure.CloseAfterAccept -> Unit
                    RawUpstreamFailure.CloseAfterRequest -> signals.requestRead.await()
                    RawUpstreamFailure.CloseAfterStatusOnlyResponse -> signals.statusOnlyResponseWritten.await()
                    RawUpstreamFailure.CloseAfterResponseHeaders,
                    RawUpstreamFailure.CloseAfterPartialBody -> error("Unexpected failure case: $failure")
                }
                signals.disconnectRequested.complete(Unit)
                signals.disconnected.await()
                downstreamSignals.responseHeadersRead.await()
                downstreamSignals.responseBodyRead.await()

                val (status, bodyText) = downstreamResponse.await()
                assertEquals(HttpStatusCode.GatewayTimeout, status, failure.name)
                val body = Json.parseToJsonElement(bodyText).jsonObject
                val error = body.getValue("error").jsonObject
                assertEquals("upstream_timeout", error.getValue("type").jsonPrimitive.content)
                assertEquals("Upstream timed out", error.getValue("message").jsonPrimitive.content)
            }
        }
    }

    @Test
    fun `passthrough keeps status when upstream disconnects after status-only response`() = runBlocking {
        withFailingRawUpstream(RawUpstreamFailure.CloseAfterStatusOnlyResponse) { downstreamPort, signals ->
            val downstreamSignals = RawDownstreamSignals()
            val downstreamResponse = async {
                postPassthroughRaw(downstreamPort, signals = downstreamSignals)
            }

            downstreamSignals.requestSent.await()
            signals.accepted.await()
            signals.requestRead.await()
            signals.statusOnlyResponseWritten.await()
            downstreamSignals.responseHeadersRead.await()
            signals.disconnectRequested.complete(Unit)
            signals.disconnected.await()
            downstreamSignals.responseBodyRead.await()

            val (status, bodyText) = downstreamResponse.await()
            assertEquals(HttpStatusCode.OK, status)
            assertEquals("", bodyText)
        }
    }

    @Test
    fun `passthrough keeps response when upstream disconnects after response headers`() = runBlocking {
        withFailingRawUpstream(RawUpstreamFailure.CloseAfterResponseHeaders) { downstreamPort, signals ->
            val downstreamSignals = RawDownstreamSignals()
            val downstreamResponse = async {
                postPassthroughRaw(downstreamPort, signals = downstreamSignals)
            }

            downstreamSignals.requestSent.await()
            signals.accepted.await()
            signals.requestRead.await()
            signals.responseHeadersWritten.await()
            downstreamSignals.responseHeadersRead.await()
            signals.disconnectRequested.complete(Unit)
            signals.disconnected.await()
            downstreamSignals.responseBodyRead.await()

            val (status, bodyText) = downstreamResponse.await()
            assertEquals(HttpStatusCode.OK, status)
            assertEquals("", bodyText)
        }
    }

    @Test
    fun `passthrough keeps partial close-delimited body when upstream disconnects during body`() = runBlocking {
        withFailingRawUpstream(RawUpstreamFailure.CloseAfterPartialBody) { downstreamPort, signals ->
            val downstreamSignals = RawDownstreamSignals()
            val downstreamResponse = async {
                postPassthroughRaw(
                    downstreamPort,
                    expectedBodyFragment = "partial",
                    signals = downstreamSignals,
                )
            }

            downstreamSignals.requestSent.await()
            signals.accepted.await()
            signals.requestRead.await()
            signals.responseHeadersWritten.await()
            signals.partialBodyWritten.await()
            downstreamSignals.responseHeadersRead.await()
            downstreamSignals.expectedBodyFragmentRead.await()
            signals.disconnectRequested.complete(Unit)
            signals.disconnected.await()
            downstreamSignals.responseBodyRead.await()

            val (status, bodyText) = downstreamResponse.await()
            assertEquals(HttpStatusCode.OK, status)
            assertEquals("partial", bodyText)
        }
    }
}
