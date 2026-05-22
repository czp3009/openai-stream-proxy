package com.hiczp.openai.stream.proxy

import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
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
        assertEquals("chat.completion", response.getValue("object").jsonPrimitive.content)
        assertEquals("chatcmpl-202605221412347747663898268d9d613pN8kA5", response.getValue("id").jsonPrimitive.content)
        assertEquals("gpt-5.4", response.getValue("model").jsonPrimitive.content)
        assertEquals(20, response.getValue("usage").jsonObject.getValue("total_tokens").jsonPrimitive.int)

        val choice = response.getValue("choices").jsonArray.single().jsonObject
        assertEquals(0, choice.getValue("index").jsonPrimitive.int)
        assertEquals("stop", choice.getValue("finish_reason").jsonPrimitive.content)

        val message = choice.getValue("message").jsonObject
        assertEquals("assistant", message.getValue("role").jsonPrimitive.content)
        assertEquals("Hello! How can I help you today?", message.getValue("content").jsonPrimitive.content)
    }
}
