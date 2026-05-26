package com.hiczp.openai.stream.proxy

import io.ktor.client.*
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
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds
import io.ktor.server.cio.CIO as ServerCIO

class PassthroughDisconnectTest {
    private enum class RawUpstreamFailure {
        CloseAfterAccept,
        CloseAfterRequest,
        CloseAfterStatusLine,
        CloseAfterResponseHeaders,
        CloseAfterPartialBody,
    }

    private data class RawUpstreamSignals(
        val accepted: CompletableDeferred<Unit> = CompletableDeferred(),
        val requestRead: CompletableDeferred<Unit> = CompletableDeferred(),
        val statusLineWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val responseHeadersWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val partialBodyWritten: CompletableDeferred<Unit> = CompletableDeferred(),
        val disconnected: CompletableDeferred<Unit> = CompletableDeferred(),
    ) {
        fun completeExceptionally(cause: Throwable) {
            accepted.completeExceptionally(cause)
            requestRead.completeExceptionally(cause)
            statusLineWritten.completeExceptionally(cause)
            responseHeadersWritten.completeExceptionally(cause)
            partialBodyWritten.completeExceptionally(cause)
            disconnected.completeExceptionally(cause)
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
                            RawUpstreamFailure.CloseAfterRequest -> Unit

                            RawUpstreamFailure.CloseAfterStatusLine -> {
                                output.write("HTTP/1.1 200 OK\r\n".encodeToByteArray())
                                output.flush()
                                signals.statusLineWritten.complete(Unit)
                            }

                            RawUpstreamFailure.CloseAfterResponseHeaders -> {
                                writePlainResponseHeaders(output)
                                signals.responseHeadersWritten.complete(Unit)
                            }

                            RawUpstreamFailure.CloseAfterPartialBody -> {
                                writePlainResponseHeaders(output)
                                signals.responseHeadersWritten.complete(Unit)
                                output.write("partial".encodeToByteArray())
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
        val proxy = ChatCompletionsApiProxy(engine, "http://127.0.0.1:$upstreamPort")
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

    private suspend fun postPassthrough(downstreamPort: Int): Pair<HttpStatusCode, String> {
        val client = HttpClient(CIO.create())
        return try {
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/other") {
                contentType(ContentType.Application.Json)
                setBody(buildJsonObject { put("model", "gpt-4") }.toString())
            }
            response.status to response.bodyAsText()
        } finally {
            client.close()
        }
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
    fun `passthrough returns 502 when upstream disconnects before usable response`() = runBlocking {
        val failures = listOf(
            RawUpstreamFailure.CloseAfterAccept,
            RawUpstreamFailure.CloseAfterRequest,
        )

        failures.forEach { failure ->
            withFailingRawUpstream(failure) { downstreamPort, signals ->
                val downstreamResponse = async { postPassthrough(downstreamPort) }

                withTimeout(2.seconds) {
                    signals.accepted.await()
                    when (failure) {
                        RawUpstreamFailure.CloseAfterAccept -> Unit
                        RawUpstreamFailure.CloseAfterRequest -> signals.requestRead.await()
                        RawUpstreamFailure.CloseAfterStatusLine -> signals.statusLineWritten.await()
                        RawUpstreamFailure.CloseAfterResponseHeaders,
                        RawUpstreamFailure.CloseAfterPartialBody -> error("Unexpected failure case: $failure")
                    }
                    signals.disconnected.await()
                }

                val (status, bodyText) = withTimeout(2.seconds) { downstreamResponse.await() }
                assertEquals(HttpStatusCode.BadGateway, status, failure.name)
                val body = Json.parseToJsonElement(bodyText).jsonObject
                assertEquals("upstream_error", body.getValue("error").jsonObject.getValue("type").jsonPrimitive.content)
            }
        }
    }

    @Test
    fun `passthrough keeps status when upstream disconnects after status line`() = runBlocking {
        withFailingRawUpstream(RawUpstreamFailure.CloseAfterStatusLine) { downstreamPort, signals ->
            val downstreamResponse = async { postPassthrough(downstreamPort) }

            withTimeout(2.seconds) {
                signals.accepted.await()
                signals.requestRead.await()
                signals.statusLineWritten.await()
                signals.disconnected.await()
            }

            val (status, bodyText) = withTimeout(2.seconds) { downstreamResponse.await() }
            assertEquals(HttpStatusCode.OK, status)
            assertEquals("", bodyText)
        }
    }

    @Test
    fun `passthrough keeps response when upstream disconnects after response headers`() = runBlocking {
        withFailingRawUpstream(RawUpstreamFailure.CloseAfterResponseHeaders) { downstreamPort, signals ->
            val downstreamResponse = async { postPassthrough(downstreamPort) }

            withTimeout(2.seconds) {
                signals.accepted.await()
                signals.requestRead.await()
                signals.responseHeadersWritten.await()
                signals.disconnected.await()
            }

            val (status, bodyText) = withTimeout(2.seconds) { downstreamResponse.await() }
            assertEquals(HttpStatusCode.OK, status)
            assertEquals("", bodyText)
        }
    }

    @Test
    fun `passthrough keeps partial close-delimited body when upstream disconnects during body`() = runBlocking {
        withFailingRawUpstream(RawUpstreamFailure.CloseAfterPartialBody) { downstreamPort, signals ->
            val downstreamResponse = async { postPassthrough(downstreamPort) }

            withTimeout(2.seconds) {
                signals.accepted.await()
                signals.requestRead.await()
                signals.partialBodyWritten.await()
                signals.disconnected.await()
            }

            val (status, bodyText) = withTimeout(2.seconds) { downstreamResponse.await() }
            assertEquals(HttpStatusCode.OK, status)
            assertEquals("partial", bodyText)
        }
    }
}
