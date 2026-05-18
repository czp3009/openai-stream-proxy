package com.hiczp.openai.responses.stream.proxy.cli

import com.hiczp.openai.responses.stream.proxy.ResponsesApiProxy
import com.hiczp.openai.responses.stream.proxy.ResponsesApiProxy.Companion.errorResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.engine.cio.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.cli.ArgParser
import kotlinx.cli.ArgParser.OptionPrefixStyle
import kotlinx.cli.ArgType
import kotlinx.cli.default
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import io.ktor.server.cio.CIO as ServerCIO

private val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
    val parser = ArgParser("openai-responses-stream-proxy", prefixStyle = OptionPrefixStyle.GNU)
    val configFile by parser.option(
        ArgType.String,
        fullName = "config-file",
        shortName = "c",
        description = "Path to config JSON file",
    ).default("config.json")
    parser.parse(args)

    val rules = try {
        loadConfig(configFile)
    } catch (e: Exception) {
        logger.error { "Invalid config file: $configFile" }
        throw e
    }
    logger.info { "Loaded ${rules.size} rule(s) from $configFile" }
    if (rules.isEmpty()) {
        logger.error { "No rule found in config file: $configFile" }
        return
    }

    val engine = CIO.create()

    val servers = rules.map { rule ->
        val proxy = ResponsesApiProxy(engine, rule.upstreamUrl)
        logger.info { "Rule: port=${rule.listenPort} -> ${rule.upstreamUrl}" }

        embeddedServer(ServerCIO, port = rule.listenPort) {
            configureProxyServer(proxy)
        }.start()
    }

    registerShutdownHook {
        logger.info { "Shutting down ${servers.size} server(s)..." }
        servers.forEach { it.stop(1000L, 2000L) }
    }

    runBlocking { awaitCancellation() }
}

private fun Application.configureProxyServer(proxy: ResponsesApiProxy) {
    installErrorHandler()
    routing {
        route("/{...}") {
            handle {
                val method = call.request.httpMethod
                val uri = call.request.uri
                val path = uri.substringBefore('?').trimEnd('/')
                val upstreamUrl = proxy.upstreamBaseUrl.trimEnd('/') + uri
                logger.debug { "Handling request [${call.request.host()}:${call.request.port()} -> ${proxy.upstreamBaseUrl}]" }

                val result = if (method != HttpMethod.Post
                    || !path.endsWith("/responses")
                    || !call.request.contentType().match(ContentType.Application.Json)
                ) {
                    logger.debug { "Passthrough: ${method.value} $path (not OpenAI Responses request)" }
                    proxy.passthrough(upstreamUrl, method, call.request.headers, call.receiveChannel())
                } else {
                    proxy.proxy(method, uri, call.request.headers, call.receiveChannel())
                }

                if (result != null) {
                    call.respond(result)
                } else {
                    call.respond(
                        errorResponse(
                            message = "Upstream returned incomplete or invalid response",
                            type = "upstream_error",
                        )
                    )
                }
            }
        }
    }
}
