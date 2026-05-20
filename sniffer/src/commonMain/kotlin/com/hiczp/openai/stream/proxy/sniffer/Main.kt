package com.hiczp.openai.stream.proxy.sniffer

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
            requestTimeoutMillis = Long.MAX_VALUE
            connectTimeoutMillis = 30_000
            socketTimeoutMillis = Long.MAX_VALUE
        }
        followRedirects = false
    }
    val proxy = ReverseProxy(client, config.upstreamBaseUrl)

    embeddedServer(CIO, port = config.port) {
        routing {
            route("/{...}") {
                handle {
                    proxy.forward(call)
                }
            }
        }
    }.start(wait = true)
}
