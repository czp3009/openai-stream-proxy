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
            var upstreamIncomingClosed = false
            try {
                while (true) {
                    val frame = upstreamSession.incoming.receiveCatching().getOrNull() ?: break
                    downstreamSession.send(frame)
                }
                upstreamIncomingClosed = true
            } finally {
                withContext(NonCancellable) {
                    val upstreamCloseReason = if (upstreamIncomingClosed) {
                        (upstreamSession as? DefaultWebSocketSession)?.closeReason?.await()
                    } else {
                        null
                    }
                    val downstreamCloseReason = if (
                        upstreamCloseReason == null ||
                        upstreamCloseReason.code.toInt() == 1006
                    ) {
                        CloseReason(
                            CloseReason.Codes.INTERNAL_ERROR,
                            "upstream websocket disconnected abnormally",
                        )
                    } else {
                        upstreamCloseReason
                    }
                    downstreamSession.close(downstreamCloseReason)
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
