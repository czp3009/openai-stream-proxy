package com.hiczp.openai.stream.proxy

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.client.engine.*
import io.ktor.http.*
import io.ktor.http.content.*
import kotlinx.serialization.json.JsonObject

/**
 * Transparent proxy that forwards every downstream request to the upstream server unchanged.
 *
 * This class never enters the conversion flow. It exists so callers can make fallback routing
 * explicit without depending on a protocol-specific proxy's path-mismatch passthrough behavior.
 */
class PassthroughApiProxy(
    engine: HttpClientEngine,
    upstreamBaseUrl: String,
    timeoutMillis: Long = 600_000L,
) : AbstractApiProxy(engine, upstreamBaseUrl, timeoutMillis) {
    override val logger = KotlinLogging.logger("com.hiczp.openai.stream.proxy.PassthroughApiProxy")

    override fun needConvert(method: HttpMethod, path: String): Boolean = false

    override fun rewriteBody(bodyJson: JsonObject): JsonObject = conversionNotSupported()

    override fun createAccumulator(): SseAccumulator = conversionNotSupported()

    override fun buildResult(accumulator: SseAccumulator, sessionHeaders: Headers): OutgoingContent =
        conversionNotSupported()

    private fun conversionNotSupported(): Nothing =
        error("PassthroughApiProxy never converts requests")
}
