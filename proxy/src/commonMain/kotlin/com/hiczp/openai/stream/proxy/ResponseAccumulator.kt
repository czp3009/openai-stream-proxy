package com.hiczp.openai.stream.proxy

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.sse.*
import kotlinx.serialization.json.*

private val logger = KotlinLogging.logger("com.hiczp.openai.stream.proxy.ResponseAccumulator")

/**
 * Accumulates SSE events from an OpenAI Responses API streaming response into a single
 * non-streaming JSON response object.
 *
 * This class processes [ServerSentEvent]s in sequence, collecting output items and waiting
 * for a terminal event (`response.completed`, `response.failed`, or `response.incomplete`)
 * to produce the final response. It is **not thread-safe** — callers must ensure
 * [accumulate] is called from a single thread or coordinate access externally.
 */
class ResponseAccumulator {
    private val outputArray = mutableListOf<JsonElement?>()

    /** The type of the terminal event, or `null` if no terminal event has been received yet. */
    var terminalType: TerminalType? = null
        private set

    private var syntheticResponse: JsonObject? = null

    /** Whether a terminal event has been received and accumulation is finished. */
    val isTerminated: Boolean get() = syntheticResponse != null

    /**
     * The aggregated response JSON.
     * @throws IllegalStateException if no terminal event has been received yet.
     */
    val response: JsonObject get() = checkNotNull(syntheticResponse) { "No terminal event received yet" }

    /** The type of terminal event that ended the SSE stream. */
    enum class TerminalType {
        /** The response was completed successfully (`response.completed`). */
        COMPLETED,

        /** The response failed (`response.failed`). */
        FAILED,

        /** The response ended incompletely (`response.incomplete`). */
        INCOMPLETE,
    }

    /**
     * Feeds a single [ServerSentEvent] into the accumulator.
     * Events are processed only until a terminal event is received; subsequent calls are no-ops.
     */
    fun accumulate(event: ServerSentEvent) {
        if (isTerminated) {
            logger.trace { "Dropping event after termination: ${event.event}" }
            return
        }

        val data = event.data ?: run {
            logger.trace { "Dropping event with no data: ${event.event}" }
            return
        }
        val jsonData = Json.parseToJsonElement(data).jsonObject

        when (event.event) {
            "response.output_item.done" -> {
                val outputIndex = jsonData["output_index"]?.jsonPrimitive?.int ?: return
                val item = jsonData["item"] ?: return
                logger.trace { "Output item done: index=$outputIndex, type=${item.jsonObject["type"]}" }
                ensureCapacity(outputIndex + 1)
                outputArray[outputIndex] = item
            }

            "response.completed" -> {
                val upstreamResponse = jsonData["response"]?.jsonObject ?: return
                val output = upstreamResponse["output"]
                val noOutput = output == null || output is JsonNull || (output is JsonArray && output.isEmpty())
                syntheticResponse = if (noOutput) {
                    logger.trace { "Response completed: assembling ${outputArray.size} output items from accumulator" }
                    JsonObject(upstreamResponse + ("output" to JsonArray(outputArray.mapNotNull { it })))
                } else {
                    logger.trace { "Response completed: using upstream output directly" }
                    upstreamResponse
                }
                terminalType = TerminalType.COMPLETED
            }

            "response.failed" -> {
                logger.trace { "Response failed: ${jsonData["error"]?.jsonObject?.get("message")?.jsonPrimitive?.contentOrNull}" }
                syntheticResponse = jsonData["response"]?.jsonObject ?: return
                terminalType = TerminalType.FAILED
            }

            "response.incomplete" -> {
                logger.trace { "Response incomplete: ${jsonData["incomplete_details"]?.jsonObject?.get("reason")?.jsonPrimitive?.contentOrNull}" }
                syntheticResponse = jsonData["response"]?.jsonObject ?: return
                terminalType = TerminalType.INCOMPLETE
            }
        }
    }

    private fun ensureCapacity(minSize: Int) {
        while (outputArray.size < minSize) {
            outputArray.add(null)
        }
    }
}
