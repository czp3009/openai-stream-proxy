package com.hiczp.openai.stream.proxy

import kotlinx.serialization.json.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ResponseAccumulatorTest {
    @Test
    fun `response accumulator reconstructs completed response output from done items`() {
        val accumulator = ResponseAccumulator()

        sseEvents("responses_sse.txt").forEach(accumulator::accumulate)

        assertTrue(accumulator.isTerminated)
        assertEquals(ResponseAccumulator.TerminalType.COMPLETED, accumulator.terminalType)

        val response = accumulator.response
        assertEquals("completed", response.getValue("status").jsonPrimitive.content)
        assertEquals(20, response.getValue("usage").jsonObject.getValue("total_tokens").jsonPrimitive.int)

        val message = response.getValue("output").jsonArray.single().jsonObject
        assertEquals("message", message.getValue("type").jsonPrimitive.content)
        assertEquals("completed", message.getValue("status").jsonPrimitive.content)
        assertEquals("assistant", message.getValue("role").jsonPrimitive.content)

        val content = message.getValue("content").jsonArray.single().jsonObject
        assertEquals("output_text", content.getValue("type").jsonPrimitive.content)
        assertEquals("Hello! How can I help you today?", content.getValue("text").jsonPrimitive.content)
    }

    @Test
    fun `response accumulator records failed terminal response with output fallback`() {
        val accumulator = ResponseAccumulator()

        sseEvents("responses_failed_sse.txt").forEach(accumulator::accumulate)

        assertTrue(accumulator.isTerminated)
        assertEquals(ResponseAccumulator.TerminalType.FAILED, accumulator.terminalType)
        assertEquals("failed", accumulator.response.getValue("status").jsonPrimitive.content)
        assertEquals(
            "server_error",
            accumulator.response.getValue("error").jsonObject.getValue("code").jsonPrimitive.content
        )
        assertEquals("failed partial output", accumulator.response.outputText())
    }

    @Test
    fun `response accumulator records incomplete terminal response with output fallback`() {
        val accumulator = ResponseAccumulator()

        sseEvents("responses_incomplete_sse.txt").forEach(accumulator::accumulate)

        assertTrue(accumulator.isTerminated)
        assertEquals(ResponseAccumulator.TerminalType.INCOMPLETE, accumulator.terminalType)
        assertEquals("incomplete", accumulator.response.getValue("status").jsonPrimitive.content)
        assertEquals(
            "max_output_tokens",
            accumulator.response.getValue("incomplete_details").jsonObject.getValue("reason").jsonPrimitive.content,
        )
        assertEquals("incomplete partial output", accumulator.response.outputText())
    }
}

private fun JsonObject.outputText() = getValue("output").jsonArray
    .flatMap { it.jsonObject.getValue("content").jsonArray }
    .joinToString("") { it.jsonObject.getValue("text").jsonPrimitive.content }
