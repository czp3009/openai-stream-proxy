package com.hiczp.openai.stream.proxy.cli

import com.hiczp.openai.stream.proxy.OpenAiErrors
import com.hiczp.openai.stream.proxy.ResponsesApiProxy
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
import java.net.ServerSocket
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import io.ktor.server.cio.CIO as ServerCIO

class ServerTest {
    private val sseResponseText: ByteArray by lazy {
        javaClass.getResourceAsStream("/com/hiczp/openai/stream/proxy/cli/responses_sse.txt")!!.readAllBytes()
    }

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }

    @Test
    fun `proxy returns aggregated response for valid SSE upstream`() = runBlocking {
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
            installErrorHandler()
            routing {
                route("/{...}") {
                    handle {
                        val result = proxy.proxy(
                            requestMethod = call.request.httpMethod,
                            requestUri = call.request.uri,
                            requestHeaders = call.request.headers,
                            requestBody = call.receiveChannel(),
                        )
                        if (result != null) {
                            call.respond(result)
                        } else {
                            call.respond(
                                OpenAiErrors.errorResponse(
                                    message = "Upstream returned incomplete or invalid response",
                                    type = "upstream_error",
                                )
                            )
                        }
                    }
                }
            }
        }.start()

        val client = HttpClient(CIO)
        try {
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody("""{"model":"gpt-4","input":"hello"}""")
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
            installErrorHandler()
            routing {
                route("/{...}") {
                    handle {
                        val result = proxy.proxy(
                            requestMethod = call.request.httpMethod,
                            requestUri = call.request.uri,
                            requestHeaders = call.request.headers,
                            requestBody = call.receiveChannel(),
                        )
                        if (result != null) {
                            call.respond(result)
                        } else {
                            call.respond(
                                OpenAiErrors.errorResponse(
                                    message = "Upstream returned incomplete or invalid response",
                                    type = "upstream_error",
                                )
                            )
                        }
                    }
                }
            }
        }.start()

        val client = HttpClient(CIO)
        try {
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody("""{"model":"gpt-4","input":"hello"}""")
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
    fun `returns 500 when upstream is unreachable`() = runBlocking {
        val downstreamPort = findFreePort()
        val unreachablePort = findFreePort()

        val proxy = ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$unreachablePort")

        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
            installErrorHandler()
            routing {
                route("/{...}") {
                    handle {
                        val result = proxy.proxy(
                            requestMethod = call.request.httpMethod,
                            requestUri = call.request.uri,
                            requestHeaders = call.request.headers,
                            requestBody = call.receiveChannel(),
                        )
                        if (result != null) {
                            call.respond(result)
                        } else {
                            call.respond(
                                OpenAiErrors.errorResponse(
                                    message = "Upstream returned incomplete or invalid response",
                                    type = "upstream_error",
                                )
                            )
                        }
                    }
                }
            }
        }.start()

        val client = HttpClient(CIO)
        try {
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/responses") {
                contentType(ContentType.Application.Json)
                setBody("""{"model":"gpt-4","input":"hello"}""")
            }
            assertEquals(HttpStatusCode.InternalServerError, response.status)
            val body = response.bodyAsText()
            assertTrue(body.contains("internal_error"))
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

        val proxies = mapOf(
            portA to ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPortA"),
            portB to ResponsesApiProxy(CIO.create(), "http://127.0.0.1:$upstreamPortB"),
        )

        val server = embeddedServer(
            ServerCIO,
            configure = { proxies.keys.forEach { connector { port = it } } }
        ) { configureProxyServer(proxies) }.start()

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
