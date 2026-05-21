package com.hiczp.openai.stream.proxy

import kotlinx.serialization.json.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ChatCompletionsAccumulatorTest {
    @Test
    fun `chat completions accumulator merges streamed deltas and preserves extensions`() {
        val accumulator = ChatCompletionsAccumulator()

        sseEvents("chat_completions_sse.txt").forEach(accumulator::accumulate)

        assertTrue(accumulator.isTerminated)

        val response = accumulator.response
        assertEquals("chat.completion", response.str("object"))
        assertEquals("chatcmpl-123", response.str("id"))
        assertEquals("gpt-4o-mini", response.str("model"))
        assertEquals(42, response.getValue("usage").jsonObject.int("total_tokens"))

        val providerTop = response.getValue("provider_top").jsonObject
        assertTrue(providerTop.bool("first"))
        assertTrue(providerTop.bool("second"))

        val choice = response.getValue("choices").jsonArray.single().jsonObject
        assertEquals(0, choice.int("index"))
        assertEquals("stop", choice.str("finish_reason"))
        assertEquals(2, choice.getValue("logprobs").jsonObject.getValue("content").jsonArray.size)
        assertEquals("choice-extension", choice.getValue("provider_choice").jsonObject.str("note"))

        val message = choice.getValue("message").jsonObject
        assertEquals("assistant", message.str("role"))
        assertEquals("Hello!", message.str("content"))
        assertEquals(3, message.int("debug_tokens"))
        assertEquals("a", message.getValue("vendor").jsonObject.str("trace"))

        val functionCall = message.getValue("function_call").jsonObject
        assertEquals("lookup_weather", functionCall.str("name"))
        assertEquals("""{"city":"Boston"}""", functionCall.str("arguments"))

        val toolCall = message.getValue("tool_calls").jsonArray.single().jsonObject
        assertEquals(0, toolCall.int("index"))
        assertEquals("call_123", toolCall.str("id"))
        assertEquals("function", toolCall.str("type"))
        assertEquals("get_weather", toolCall.getValue("function").jsonObject.str("name"))
        assertEquals("""{"location":"Boston, MA"}""", toolCall.getValue("function").jsonObject.str("arguments"))
    }
}

private fun JsonObject.str(name: String) = getValue(name).jsonPrimitive.content

private fun JsonObject.int(name: String) = getValue(name).jsonPrimitive.int

private fun JsonObject.bool(name: String) = getValue(name).jsonPrimitive.boolean
