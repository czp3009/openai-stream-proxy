package com.hiczp.openai.responses.stream.proxy.cli

import com.hiczp.openai.responses.stream.proxy.ResponsesApiProxy
import com.hiczp.openai.responses.stream.proxy.ResponsesApiProxy.Companion.errorResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.engine.cio.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.http.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.util.*
import kotlinx.cli.ArgParser
import kotlinx.cli.ArgParser.OptionPrefixStyle
import kotlinx.cli.ArgType
import kotlinx.cli.default
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.runBlocking
import kotlin.time.DurationUnit
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import io.ktor.server.cio.CIO as ServerCIO

private val logger = KotlinLogging.logger {}
private val timeSource = TimeSource.Monotonic
internal val RequestStartMarkKey = AttributeKey<TimeMark>("RequestStartMark")

fun main(args: Array<String>) {
    configureLogging()

    val parser = ArgParser("openai-responses-stream-proxy", prefixStyle = OptionPrefixStyle.GNU)
    val configFile by parser.option(
        ArgType.String,
        fullName = "config-file",
        shortName = "c",
        description = "Path to config JSON file",
    ).default("config.json")
    parser.parse(args)

    val config = try {
        loadConfig(configFile)
    } catch (e: Exception) {
        logger.error { "Invalid config file: $configFile" }
        throw e
    }
    val rules = config.rules
    logger.info { "Loaded ${rules.size} rule(s) from $configFile" }
    if (rules.isEmpty()) {
        logger.error { "No rule found in config file: $configFile" }
        return
    }

    val engine = CIO.create()
    val timeoutSeconds = config.timeoutSeconds

    val servers = rules.map { rule ->
        val proxy = ResponsesApiProxy(engine, rule.upstreamUrl, timeoutMillis = timeoutSeconds * 1000)
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
    install(HttpRequestLifecycle) {
        cancelCallOnClose = true
    }
    installErrorHandler()
    routing {
        route("/{...}") {
            handle {
                val startMark = timeSource.markNow()
                call.attributes.put(RequestStartMarkKey, startMark)

                val method = call.request.httpMethod
                val uri = call.request.uri
                val path = uri.substringBefore('?').trimEnd('/')
                val upstreamUrl = proxy.upstreamBaseUrl.trimEnd('/') + uri

                logger.info { "Request [${call.request.host()}:${call.request.port()} -> ${proxy.upstreamBaseUrl}] ${method.value} $uri" }

                val result = if (method != HttpMethod.Post
                    || !path.endsWith("/responses")
                    || !call.request.contentType().match(ContentType.Application.Json)
                ) {
                    logger.info { "Direct forward: ${method.value} $path (not OpenAI Responses request)" }
                    proxy.passthrough(upstreamUrl, method, call.request.headers, call.receiveChannel())
                } else {
                    proxy.proxy(method, uri, call.request.headers, call.receiveChannel())
                }

                val statusCode = if (result != null) {
                    call.respond(result)
                    result.status
                } else {
                    val errorResponse = errorResponse(
                        message = "Upstream returned incomplete or invalid response",
                        type = "upstream_error",
                    )
                    call.respond(errorResponse)
                    errorResponse.status
                }
                val elapsed = startMark.elapsedNow().toInt(DurationUnit.MILLISECONDS)
                logger.info { "${method.value} $uri ${statusCode?.value ?: "<UnknownStatus>"} (${elapsed}ms)" }
            }
        }
    }
}
