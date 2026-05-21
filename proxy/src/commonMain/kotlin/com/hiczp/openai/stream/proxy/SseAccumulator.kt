package com.hiczp.openai.stream.proxy

import io.ktor.sse.*
import kotlinx.serialization.json.JsonObject

/**
 * Accumulates [ServerSentEvent]s from an SSE stream into a single aggregated JSON response.
 *
 * Implementations consume events sequentially via [accumulate] until a terminal condition is reached,
 * at which point [response] becomes available. Callers must invoke [accumulate] from a single
 * coroutine (implementations are not thread-safe).
 */
interface SseAccumulator {
    /**
     * Whether a terminal condition has been reached and accumulation is finished.
     */
    val isTerminated: Boolean

    /**
     * The aggregated response JSON.
     *
     * @throws IllegalStateException if accumulation has not produced a terminal response yet.
     */
    val response: JsonObject

    /**
     * Feeds a single [ServerSentEvent] into the accumulator.
     *
     * Implementations should ignore events after reaching their terminal condition (subsequent
     * calls are no-ops).
     *
     * Implementations are **not** thread-safe — callers must ensure this method is called from a
     * single coroutine or coordinate access externally.
     */
    fun accumulate(event: ServerSentEvent)
}
