package com.hiczp.openai.responses.stream.proxy.cli

import com.hiczp.openai.responses.stream.proxy.ResponsesApiProxy
import com.hiczp.openai.responses.stream.proxy.ResponsesApiProxy.Companion.errorResponse
import io.github.oshai.kotlinlogging.KotlinLogging
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
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlin.time.DurationUnit
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import io.ktor.server.cio.CIO as ServerCIO

private val logger = KotlinLogging.logger("com.hiczp.openai.responses.stream.proxy.cli.Application")
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
    val clientEngine = createClientEngine()
    val proxies = config.rules.associate { rule ->
        rule.listenPort to ResponsesApiProxy(
            engine = clientEngine,
            upstreamBaseUrl = rule.upstreamUrl,
            timeoutMillis = config.timeoutSeconds * 1_000,
        )
    }
    logger.info { "Loaded ${proxies.size} rule(s) from $configFile" }
    if (proxies.isEmpty()) {
        logger.error { "No rule found in config file: $configFile" }
        return
    }
    proxies.forEach { (port, proxy) ->
        logger.info { "Proxy: port=$port -> ${proxy.upstreamBaseUrl}" }
    }

    val server = try {
        embeddedServer(
            ServerCIO,
            configure = {
                proxies.keys.forEach { listenPort -> connector { port = listenPort } }
            }
        ) {
            configureProxyServer(proxies)
        }.start()
    } catch (e: Exception) {
        tailrec fun unwrapRootCause(e: Throwable): Throwable {
            val cause = e.cause ?: return e
            if (cause === e) return e
            if (e !is CancellationException) return e
            return unwrapRootCause(cause)
        }

        val message = unwrapRootCause(e).message ?: e.message
        logger.error { "Failed to start server: $message" }
        exitProcess(1)
    }

    val shutdownRequest = CompletableDeferred<Unit>()
    registerShutdownHook { shutdownRequest.complete(Unit) }

    runBlocking {
        shutdownRequest.await()
        logger.info { "Shutting down server..." }
        server.stop(2_000L, 5_000L)
    }
}

internal fun Application.configureProxyServer(proxies: Map<Int, ResponsesApiProxy>) {
    install(HttpRequestLifecycle) {
        cancelCallOnClose = true
    }
    installErrorHandler()
    routing {
        route("/{...}") {
            handle {
                val startMark = timeSource.markNow()
                call.attributes.put(RequestStartMarkKey, startMark)

                val proxy = proxies.getValue(call.request.local.localPort)

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
