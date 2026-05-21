package com.hiczp.openai.stream.proxy

import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.sse.*
import kotlinx.serialization.json.*

private val logger = KotlinLogging.logger("com.hiczp.openai.stream.proxy.ChatCompletionsAccumulator")

/**
 * Accumulates SSE events from an OpenAI Chat Completions API streaming response into a single
 * non-streaming JSON response object.
 *
 * Uses generic JSON delta merging inside streamed chunks instead of enumerating every known
 * OpenAI message/tool-call field, preserving third-party provider extensions and future protocol
 * additions.
 *
 * Top-level fields (e.g. `metadata`, `usage`, `system_fingerprint`) are accumulated across
 * chunks via deep merge, so fields that only appear on certain chunks are preserved in the final
 * response. The streaming `object` value is replaced with `chat.completion` during assembly.
 *
 * Not thread-safe - callers must ensure [accumulate] is called from a single coroutine.
 */
class ChatCompletionsAccumulator : SseAccumulator {
    private val choices = mutableMapOf<Int, ChoiceState>()
    private val topLevelFields = mutableMapOf<String, JsonElement>()
    private var assembledResponse: JsonObject? = null

    /** Whether a final non-streaming response has been assembled. */
    val isTerminated: Boolean get() = assembledResponse != null

    /**
     * The aggregated response JSON.
     * @throws IllegalStateException if no final response has been assembled yet.
     */
    override val response: JsonObject get() = checkNotNull(assembledResponse) { "No final response assembled yet" }

    private class ChoiceState(
        val message: MutableMap<String, JsonElement> = mutableMapOf(),
        val fields: MutableMap<String, JsonElement> = mutableMapOf(),
    )

    /**
     * Feeds a single [ServerSentEvent] into the accumulator.
     * Events are processed until a final response is assembled; subsequent calls are no-ops.
     */
    override fun accumulate(event: ServerSentEvent) {
        if (isTerminated) return

        val data = event.data ?: return
        if (data.trim() == "[DONE]") {
            assembleResponse()
            return
        }

        val chunk = Json.parseToJsonElement(data).jsonObject
        if (
            chunk["choices"] !is JsonArray &&
            chunk["object"]?.jsonPrimitive?.contentOrNull != "chat.completion.chunk"
        ) return

        chunk.forEach { (key, value) ->
            if (key != "choices" && key != "object") {
                topLevelFields[key] = topLevelFields[key].deepMergeWith(value)
            }
        }

        (chunk["choices"] as? JsonArray)?.forEach { choiceElement ->
            val choice = choiceElement as? JsonObject ?: return@forEach
            val index = choice["index"]?.jsonPrimitive?.intOrNull ?: return@forEach
            val state = choices.getOrPut(index) { ChoiceState() }

            (choice["delta"] as? JsonObject)?.let { delta ->
                val merged = JsonObject(state.message).accumulateDelta(delta)
                state.message.clear()
                state.message.putAll(merged)
            }

            choice.forEach { (key, value) ->
                if (key != "delta" && key != "index") {
                    state.fields[key] = state.fields[key].deepMergeWith(value)
                }
            }
        }
    }

    private fun assembleResponse() {
        if (topLevelFields.isEmpty()) {
            logger.warn { "Cannot assemble response: no chunks received" }
            return
        }

        val sortedIndices = choices.keys.sorted()
        val choicesArray = JsonArray(sortedIndices.map { index ->
            val state = choices.getValue(index)
            buildJsonObject {
                put("index", index)
                put("message", JsonObject(state.message))
                state.fields.forEach { (k, v) -> put(k, v) }
            }
        })

        assembledResponse = JsonObject(buildMap {
            putAll(topLevelFields)
            put("object", JsonPrimitive("chat.completion"))
            put("choices", choicesArray)
        })
        logger.trace { "Assembled response with ${choicesArray.size} choices" }
    }
}

/**
 * Recursive JSON delta merge following the OpenAI SDK delta accumulation rules used by streamed
 * Chat Completions chunks.
 *
 * Strings and numbers are accumulated, arrays are extended, object entries with an `index` key are
 * merged at that index, and `index` / `type` keys are always replaced because they identify union
 * members.
 */
private fun JsonObject.accumulateDelta(delta: JsonObject): JsonObject =
    JsonObject(toMutableMap().apply {
        delta.forEach { (key, value) ->
            this[key] = this[key].mergeDeltaValue(key, value)
        }
    })

private fun JsonElement?.mergeDeltaValue(key: String, delta: JsonElement): JsonElement = when {
    this == null || this is JsonNull -> delta
    delta is JsonNull -> this
    key == "index" || key == "type" -> delta
    this is JsonPrimitive && delta is JsonPrimitive && isString && delta.isString ->
        JsonPrimitive(content + delta.content)

    this is JsonPrimitive && delta is JsonPrimitive && isNumber && delta.isNumber ->
        plus(delta)

    this is JsonObject && delta is JsonObject -> accumulateDelta(delta)
    this is JsonArray && delta is JsonArray -> mergeDeltaArrays(delta)
    else -> this
}

private fun JsonArray.mergeDeltaArrays(delta: JsonArray): JsonArray {
    if (all { it is JsonPrimitive }) return JsonArray(plus(delta))
    return toMutableList().apply {
        delta.forEach { entry ->
            if (entry !is JsonObject) {
                add(entry); return@forEach
            }
            val idx = entry["index"]?.jsonPrimitive?.intOrNull ?: run { add(entry); return@forEach }
            if (idx in indices) {
                this[idx] = (this[idx] as? JsonObject)?.accumulateDelta(entry) ?: entry
            } else {
                add(idx.coerceIn(0, size), entry)
            }
        }
    }.let(::JsonArray)
}

private val JsonPrimitive.isNumber: Boolean
    get() = !isString && booleanOrNull == null && doubleOrNull != null

private fun JsonPrimitive.plus(other: JsonPrimitive): JsonPrimitive =
    longOrNull?.let { left ->
        other.longOrNull?.let { right -> JsonPrimitive(left + right) }
    } ?: JsonPrimitive(double + other.double)

/**
 * Conservative deep merge for non-delta fields:
 * objects are recursively merged, arrays are extended, primitives are replaced.
 */
private fun JsonElement?.deepMergeWith(delta: JsonElement): JsonElement = when {
    this == null || this is JsonNull -> delta
    delta is JsonNull -> this
    this is JsonObject && delta is JsonObject ->
        JsonObject(toMutableMap().apply {
            delta.forEach { (k, v) -> this[k] = this[k].deepMergeWith(v) }
        })

    this is JsonArray && delta is JsonArray -> JsonArray(plus(delta))
    else -> delta
}
