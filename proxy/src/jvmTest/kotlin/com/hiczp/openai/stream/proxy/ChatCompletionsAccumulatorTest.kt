package com.hiczp.openai.stream.proxy

import kotlinx.serialization.json.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ChatCompletionsAccumulatorTest {
    @Test
    fun `chat completions accumulator merges streamed deltas into non-streaming response`() {
        val accumulator = ChatCompletionsAccumulator()

        sseEvents("chat_completions_sse.txt").forEach(accumulator::accumulate)

        assertTrue(accumulator.isTerminated)

        val response = accumulator.response
        assertEquals("chat.completion", response.str("object"))
        assertEquals("chatcmpl-202605221412347747663898268d9d613pN8kA5", response.str("id"))
        assertEquals("gpt-5.4", response.str("model"))
        assertEquals(20, response.getValue("usage").jsonObject.int("total_tokens"))

        val choice = response.getValue("choices").jsonArray.single().jsonObject
        assertEquals(0, choice.int("index"))
        assertEquals("stop", choice.str("finish_reason"))

        val message = choice.getValue("message").jsonObject
        assertEquals("assistant", message.str("role"))
        assertEquals("Hello! How can I help you today?", message.str("content"))
    }
}

private fun JsonObject.str(name: String) = getValue(name).jsonPrimitive.content

private fun JsonObject.int(name: String) = getValue(name).jsonPrimitive.int

private fun JsonObject.bool(name: String) = getValue(name).jsonPrimitive.boolean
