package com.hiczp.openai.stream.proxy.cli

import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.util.*
import io.ktor.websocket.*
import kotlinx.coroutines.*

private val webSocketHopByHopHeaderNames = setOf(
    "host",
    "connection",
    "transfer-encoding",
    "upgrade",
    "sec-websocket-accept",
    "sec-websocket-extensions",
    "sec-websocket-key",
    "sec-websocket-protocol",
    "sec-websocket-version",
)

internal fun HttpRequestBuilder.appendForwardedWebSocketHeaders(requestHeaders: Headers) {
    val connectionHeaderNames = requestHeaders[HttpHeaders.Connection]
        ?.split(",")
        ?.map { it.trim().lowercase() }
        ?.toSet()
        ?: emptySet()

    val forwardedHeaders = requestHeaders
        .filter { name, _ -> name.lowercase() !in webSocketHopByHopHeaderNames }
        .filter { name, _ -> name.lowercase() !in connectionHeaderNames }

    headers.appendAll(forwardedHeaders)
}

internal suspend fun proxyWebSocketSessions(
    downstreamSession: WebSocketSession,
    upstreamSession: WebSocketSession,
) {
    coroutineScope {
        val downstreamToUpstream = launch {
            try {
                for (frame in downstreamSession.incoming) {
                    upstreamSession.send(frame)
                }
            } finally {
                withContext(NonCancellable) {
                    upstreamSession.close()
                }
                throw CancellationException("Downstream WebSocket session closed")
            }
        }
        val upstreamToDownstream = launch {
            try {
                for (frame in upstreamSession.incoming) {
                    downstreamSession.send(frame)
                }
            } finally {
                withContext(NonCancellable) {
                    downstreamSession.close()
                }
                throw CancellationException("Upstream WebSocket session closed")
            }
        }

        try {
            joinAll(downstreamToUpstream, upstreamToDownstream)
        } finally {
            downstreamToUpstream.cancel()
            upstreamToDownstream.cancel()
            withContext(NonCancellable) {
                upstreamSession.close()
                downstreamSession.close()
            }
        }
    }
}
