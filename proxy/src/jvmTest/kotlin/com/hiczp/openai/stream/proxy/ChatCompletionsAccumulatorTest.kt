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
        assertEquals(42, response.obj("usage").int("total_tokens"))

        val providerTop = response.obj("provider_top")
        assertTrue(providerTop.bool("first"))
        assertTrue(providerTop.bool("second"))

        val choice = response.arr("choices").single().jsonObject
        assertEquals(0, choice.int("index"))
        assertEquals("stop", choice.str("finish_reason"))
        assertEquals(2, choice.obj("logprobs").arr("content").size)
        assertEquals("choice-extension", choice.obj("provider_choice").str("note"))

        val message = choice.obj("message")
        assertEquals("assistant", message.str("role"))
        assertEquals("Hello!", message.str("content"))
        assertEquals(3, message.int("debug_tokens"))
        assertEquals("a", message.obj("vendor").str("trace"))

        val functionCall = message.obj("function_call")
        assertEquals("lookup_weather", functionCall.str("name"))
        assertEquals("""{"city":"Boston"}""", functionCall.str("arguments"))

        val toolCall = message.arr("tool_calls").single().jsonObject
        assertEquals(0, toolCall.int("index"))
        assertEquals("call_123", toolCall.str("id"))
        assertEquals("function", toolCall.str("type"))
        assertEquals("get_weather", toolCall.obj("function").str("name"))
        assertEquals("""{"location":"Boston, MA"}""", toolCall.obj("function").str("arguments"))
    }
}

private fun JsonObject.obj(name: String) = getValue(name).jsonObject

private fun JsonObject.arr(name: String) = getValue(name).jsonArray

private fun JsonObject.str(name: String) = getValue(name).jsonPrimitive.content

private fun JsonObject.int(name: String) = getValue(name).jsonPrimitive.int

private fun JsonObject.bool(name: String) = getValue(name).jsonPrimitive.boolean
