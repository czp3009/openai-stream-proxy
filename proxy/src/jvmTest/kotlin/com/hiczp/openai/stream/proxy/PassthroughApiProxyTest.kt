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
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import java.net.ServerSocket
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import io.ktor.server.cio.CIO as ServerCIO

class PassthroughApiProxyTest {
    private data class PassthroughCase(
        val path: String,
        val body: JsonObject,
    )

    private fun findFreePort(): Int = ServerSocket(0).use { it.localPort }

    @Test
    fun `passthrough proxy returns upstream status code`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                post("/v1/status") {
                    call.respondText(
                        "rate limited",
                        ContentType.Text.Plain,
                        HttpStatusCode.TooManyRequests,
                    )
                }
            }
        }.start()

        val engine = CIO.create()
        val proxy = PassthroughApiProxy(engine, "http://127.0.0.1:$upstreamPort")
        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
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
            val response = client.post("http://127.0.0.1:$downstreamPort/v1/status") {
                setBody("hello")
            }

            assertEquals(HttpStatusCode.TooManyRequests, response.status)
            assertEquals("rate limited", response.bodyAsText())
        } finally {
            client.close()
            downstreamServer.stop()
            engine.close()
            upstreamServer.stop()
        }
    }

    @Test
    fun `passthrough proxy does not rewrite conversion endpoint requests`() = runBlocking {
        val upstreamPort = findFreePort()
        val downstreamPort = findFreePort()
        val capturedBodies = ConcurrentHashMap<String, JsonObject>()

        val upstreamServer = embeddedServer(ServerCIO, port = upstreamPort) {
            routing {
                route("/{...}") {
                    handle {
                        val body = Json.parseToJsonElement(call.receiveText()).jsonObject
                        capturedBodies[call.request.uri] = body
                        call.respondText("""{"ok":true}""", ContentType.Application.Json)
                    }
                }
            }
        }.start()

        val engine = CIO.create()
        val proxy = PassthroughApiProxy(engine, "http://127.0.0.1:$upstreamPort")
        val downstreamServer = embeddedServer(ServerCIO, port = downstreamPort) {
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
            val cases = listOf(
                PassthroughCase(
                    path = "/v1/responses",
                    body = buildJsonObject {
                        put("model", "gpt-4")
                        put("input", "hello")
                    },
                ),
                PassthroughCase(
                    path = "/v1/chat/completions",
                    body = buildJsonObject {
                        put("model", "gpt-4")
                        putJsonArray("messages") {
                            add(buildJsonObject {
                                put("role", "user")
                                put("content", "hello")
                            })
                        }
                    },
                ),
            )

            cases.forEach { case ->
                val response = client.post("http://127.0.0.1:$downstreamPort${case.path}") {
                    contentType(ContentType.Application.Json)
                    setBody(case.body.toString())
                }

                assertEquals(HttpStatusCode.OK, response.status)
                assertEquals("""{"ok":true}""", response.bodyAsText())
                val forwardedBody = capturedBodies[case.path] ?: error("Upstream did not receive ${case.path}")
                assertEquals(case.body, forwardedBody)
                assertFalse("stream" in forwardedBody)
            }
        } finally {
            client.close()
            downstreamServer.stop()
            engine.close()
            upstreamServer.stop()
        }
    }
}
