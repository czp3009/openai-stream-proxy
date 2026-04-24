package com.hiczp.openai.responses.stream.proxy.sniffer

import io.ktor.client.*
import io.ktor.client.plugins.*
import io.ktor.server.cio.*
import io.ktor.server.engine.*
import io.ktor.server.routing.*

fun main() {
    val config = Config.fromEnvironment()
    config.logStartup()

    // No explicit engine — Ktor auto-discovers from classpath dependencies
    val client = HttpClient {
        install(HttpTimeout) {
            requestTimeoutMillis = 0
            connectTimeoutMillis = 30_000
            socketTimeoutMillis = 0
        }
        followRedirects = false
    }
    val proxy = ReverseProxy(client, config.upstreamUrl)

    embeddedServer(CIO, port = config.port, host = "0.0.0.0") {
        routing {
            route("/{...}") {
                handle {
                    proxy.forward(call)
                }
            }
        }
    }.start(wait = true)
}
