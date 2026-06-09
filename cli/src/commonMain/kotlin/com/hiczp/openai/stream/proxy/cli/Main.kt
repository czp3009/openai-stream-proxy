package com.hiczp.openai.stream.proxy.cli

import com.hiczp.openai.stream.proxy.AbstractApiProxy
import com.hiczp.openai.stream.proxy.ChatCompletionsApiProxy
import com.hiczp.openai.stream.proxy.PassthroughApiProxy
import com.hiczp.openai.stream.proxy.ResponsesApiProxy
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.*
import io.ktor.client.engine.*
import io.ktor.client.plugins.websocket.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.http.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.websocket.*
import io.ktor.server.websocket.WebSockets
import io.ktor.util.*
import io.ktor.websocket.*
import kotlinx.cli.ArgParser
import kotlinx.cli.ArgParser.OptionPrefixStyle
import kotlinx.cli.ArgType
import kotlinx.cli.default
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.reflect.KClass
import kotlin.time.DurationUnit
import kotlin.time.TimeMark
import kotlin.time.TimeSource
import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets
import io.ktor.client.plugins.websocket.webSocket as clientWebSocket
import io.ktor.server.cio.CIO as ServerCIO

private val logger = KotlinLogging.logger("com.hiczp.openai.stream.proxy.cli.Application")
private val timeSource = TimeSource.Monotonic
internal val RequestStartMarkKey = AttributeKey<TimeMark>("RequestStartMark")

fun main(args: Array<String>) {
    configureLogging()

    val parser = ArgParser("openai-stream-proxy", prefixStyle = OptionPrefixStyle.GNU)
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
    val timeoutMillis = config.timeoutSeconds * 1_000
    logger.info { "Loaded ${config.rules.size} rule(s) from $configFile" }
    if (config.rules.isEmpty()) {
        logger.error { "No rule found in config file: $configFile" }
        exitProcess(1)
    }
    config.rules.forEach { rule ->
        logger.info { "Proxy: port=${rule.listenPort} -> ${rule.upstreamUrl}" }
    }

    val clientEngine = createClientEngine()
    val server = try {
        embeddedServer(
            ServerCIO,
            configure = {
                config.rules.forEach { rule -> connector { port = rule.listenPort } }
            }
        ) {
            monitor.subscribe(ApplicationStopping) { clientEngine.close() }
            configureProxyServer(clientEngine, config.rules, timeoutMillis)
        }.start()
    } catch (e: Throwable) {
        clientEngine.close()

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

/**
 * Installs a catch-all route that proxies all incoming requests to the upstream servers
 * defined in [rules].
 *
 * For each request, selects the proxy implementation based on the request path:
 * paths ending with `/responses` use [ResponsesApiProxy], paths ending with
 * `/chat/completions` use [ChatCompletionsApiProxy], and all other paths use
 * [PassthroughApiProxy].
 *
 * Proxy instances are lazily created and cached by `(listen port, proxy class)`.
 * Requests with `stream=true` are not converted and are passed through unchanged.
 *
 * @param clientEngine shared [HttpClientEngine] for all upstream connections
 * @param rules the proxy rules, each mapping a listen port to an upstream base URL
 * @param timeoutMillis upstream request timeout in milliseconds
 */
internal fun Application.configureProxyServer(
    clientEngine: HttpClientEngine,
    rules: List<ProxyRule>,
    timeoutMillis: Long,
) {
    val ruleCache = rules.associateBy { it.listenPort }
    val proxyCache = mutableMapOf<Pair<Int, KClass<out AbstractApiProxy>>, AbstractApiProxy>()
    val proxyCacheLock = Mutex()
    val webSocketClient = HttpClient(clientEngine) {
        install(ClientWebSockets) {
            channels {
                incoming = bounded(capacity = 0)
                outgoing = bounded(capacity = 0)
            }
        }
    }

    install(HttpRequestLifecycle) {
        cancelCallOnClose = true
    }
    install(WebSockets) {
        channels {
            incoming = bounded(capacity = 0)
            outgoing = bounded(capacity = 0)
        }
    }
    installErrorHandler()
    routing {
        webSocket("/{...}") downstream@{
            val startMark = timeSource.markNow()
            call.attributes.put(RequestStartMarkKey, startMark)

            val port = call.request.local.localPort
            val uri = call.request.uri
            val rule = ruleCache.getValue(port)
            val upstreamUrl = URLBuilder(Url(rule.upstreamUrl)).apply {
                protocol = when (protocol) {
                    URLProtocol.HTTP -> URLProtocol.WS
                    URLProtocol.HTTPS -> URLProtocol.WSS
                    else -> protocol
                }
            }.buildString().trimEnd('/') + uri

            logger.info { "WebSocket [${call.request.host()}:${call.request.port()} -> $upstreamUrl] $uri" }

            try {
                webSocketClient.clientWebSocket({
                    url(upstreamUrl)
                    appendForwardedWebSocketHeaders(call.request.headers)
                }) {
                    proxyWebSocketSessions(downstreamSession = this@downstream, upstreamSession = this)
                }
            } catch (e: WebSocketException) {
                logger.warn { "Upstream WebSocket handshake failed for $upstreamUrl: ${e.message}" }
                close(CloseReason(CloseReason.Codes.INTERNAL_ERROR, e.message ?: "Upstream WebSocket handshake failed"))
            } finally {
                val elapsed = startMark.elapsedNow().toInt(DurationUnit.MILLISECONDS)
                logger.info { "WebSocket completed: $upstreamUrl (${elapsed}ms)" }
            }
        }

        route("/{...}") {
            handle {
                val startMark = timeSource.markNow()
                call.attributes.put(RequestStartMarkKey, startMark)

                val port = call.request.local.localPort
                val uri = call.request.uri
                val path = uri.substringBefore('?').trimEnd('/')
                val proxyClass = selectProxyClass(path)

                val proxy = proxyCacheLock.withLock {
                    proxyCache.getOrPut(port to proxyClass) {
                        val rule = ruleCache.getValue(port)
                        when (proxyClass) {
                            ResponsesApiProxy::class -> ResponsesApiProxy(
                                clientEngine,
                                rule.upstreamUrl,
                                timeoutMillis
                            )

                            ChatCompletionsApiProxy::class -> ChatCompletionsApiProxy(
                                clientEngine,
                                rule.upstreamUrl,
                                timeoutMillis,
                            )

                            else -> PassthroughApiProxy(clientEngine, rule.upstreamUrl, timeoutMillis)
                        }
                    }
                }

                val method = call.request.httpMethod
                val upstreamUrl = proxy.upstreamBaseUrl.trimEnd('/') + uri

                logger.info { "Request [${call.request.host()}:${call.request.port()} -> ${upstreamUrl}] ${method.value} $uri" }

                var statusCode: HttpStatusCode? = null
                proxy.proxy(method, uri, call.request.headers, call.receiveChannel()) { response ->
                    statusCode = response.status
                    call.respond(response)
                }

                val elapsed = startMark.elapsedNow().toInt(DurationUnit.MILLISECONDS)
                logger.info { "Request completed: ${method.value} $upstreamUrl ${statusCode?.value ?: "<UnknownStatus>"} (${elapsed}ms)" }
            }
        }
    }
}

internal fun selectProxyClass(path: String): KClass<out AbstractApiProxy> = when {
    path.endsWith("/responses") -> ResponsesApiProxy::class
    path.endsWith("/chat/completions") -> ChatCompletionsApiProxy::class
    else -> PassthroughApiProxy::class
}
