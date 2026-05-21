package com.hiczp.openai.stream.proxy

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.sse.*
import kotlinx.serialization.json.*

private val logger = KotlinLogging.logger("com.hiczp.openai.stream.proxy.ResponseAccumulator")

/**
 * Accumulates SSE events from an OpenAI Responses API stream into the terminal Response object.
 *
 * This class processes [ServerSentEvent]s in sequence, collecting output items and waiting
 * for a terminal response event (`response.completed`, `response.failed`, or `response.incomplete`).
 * If the upstream terminal `response.output` is empty, missing, or null, previously collected
 * `response.output_item.done` items are used as a fallback.
 * HTTP-level failures before the SSE stream starts are outside this class's responsibility.
 *
 * It is **not thread-safe** - callers must ensure
 * [accumulate] is called from a single thread or coordinate access externally.
 */
class ResponseAccumulator {
    private val outputArray = mutableListOf<JsonElement?>()

    /** The type of the terminal event, or `null` if no terminal response event has been received yet. */
    var terminalType: TerminalType? = null
        private set

    private var syntheticResponse: JsonObject? = null

    /** Whether a terminal response event has been received and accumulation is finished. */
    val isTerminated: Boolean get() = syntheticResponse != null

    /**
     * The aggregated Response object.
     * @throws IllegalStateException if no terminal response event has been received yet.
     */
    val response: JsonObject
        get() = checkNotNull(syntheticResponse) { "No terminal response event received yet" }

    /** The type of terminal event that ended the SSE stream. */
    enum class TerminalType {
        /** The response generation completed successfully (`response.completed`). */
        COMPLETED,

        /** The response generation failed, but still produced a valid Response object (`response.failed`). */
        FAILED,

        /** The response generation ended incompletely, but still produced a valid Response object (`response.incomplete`). */
        INCOMPLETE,
    }

    /**
     * Feeds a single [ServerSentEvent] into the accumulator.
     * Events are processed only until a terminal response event is received; subsequent
     * calls are no-ops.
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
                val outputIndex = jsonData.getValue("output_index").jsonPrimitive.int
                val item = jsonData.getValue("item")
                logger.trace { "Output item done: index=$outputIndex, type=${item.jsonObject["type"]}" }
                ensureCapacity(outputIndex + 1)
                outputArray[outputIndex] = item
            }

            "response.completed" -> {
                val upstreamResponse = jsonData.getValue("response").jsonObject
                syntheticResponse = upstreamResponse.withOutputFallback(TerminalType.COMPLETED)
                terminalType = TerminalType.COMPLETED
            }

            "response.failed" -> {
                val upstreamResponse = jsonData.getValue("response").jsonObject
                logger.trace {
                    "Response failed: ${upstreamResponse["error"]?.jsonObject?.get("message")?.jsonPrimitive?.contentOrNull}"
                }
                syntheticResponse = upstreamResponse.withOutputFallback(TerminalType.FAILED)
                terminalType = TerminalType.FAILED
            }

            "response.incomplete" -> {
                val upstreamResponse = jsonData.getValue("response").jsonObject
                logger.trace {
                    "Response incomplete: ${upstreamResponse["incomplete_details"]?.jsonObject?.get("reason")?.jsonPrimitive?.contentOrNull}"
                }
                syntheticResponse = upstreamResponse.withOutputFallback(TerminalType.INCOMPLETE)
                terminalType = TerminalType.INCOMPLETE
            }
        }
    }

    private fun JsonObject.withOutputFallback(terminalType: TerminalType): JsonObject {
        val output = this["output"]
        val noOutput = output == null || output is JsonNull || (output is JsonArray && output.isEmpty())
        return if (noOutput) {
            logger.trace { "Response $terminalType: assembling ${outputArray.size} output items from accumulator" }
            JsonObject(this + ("output" to JsonArray(outputArray.filterNotNull())))
        } else {
            logger.trace { "Response $terminalType: using upstream output directly" }
            this
        }
    }

    private fun ensureCapacity(minSize: Int) {
        while (outputArray.size < minSize) {
            outputArray.add(null)
        }
    }
}
