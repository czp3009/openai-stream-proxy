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
        assertEquals("completed", response.str("status"))
        assertEquals(19, response.getValue("usage").jsonObject.int("total_tokens"))

        val message = response.getValue("output").jsonArray.single().jsonObject
        assertEquals("message", message.str("type"))
        assertEquals("completed", message.str("status"))
        assertEquals("assistant", message.str("role"))

        val content = message.getValue("content").jsonArray.single().jsonObject
        assertEquals("output_text", content.str("type"))
        assertEquals("Hi! \uD83D\uDC4B How can I help you today?", content.str("text"))
    }

    @Test
    fun `response accumulator records failed terminal response with output fallback`() {
        val accumulator = ResponseAccumulator()

        sseEvents("responses_failed_sse.txt").forEach(accumulator::accumulate)

        assertTrue(accumulator.isTerminated)
        assertEquals(ResponseAccumulator.TerminalType.FAILED, accumulator.terminalType)
        assertEquals("failed", accumulator.response.str("status"))
        assertEquals("server_error", accumulator.response.getValue("error").jsonObject.str("code"))
        assertEquals("failed partial output", accumulator.response.outputText())
    }

    @Test
    fun `response accumulator records incomplete terminal response with output fallback`() {
        val accumulator = ResponseAccumulator()

        sseEvents("responses_incomplete_sse.txt").forEach(accumulator::accumulate)

        assertTrue(accumulator.isTerminated)
        assertEquals(ResponseAccumulator.TerminalType.INCOMPLETE, accumulator.terminalType)
        assertEquals("incomplete", accumulator.response.str("status"))
        assertEquals("max_output_tokens", accumulator.response.getValue("incomplete_details").jsonObject.str("reason"))
        assertEquals("incomplete partial output", accumulator.response.outputText())
    }
}

private fun JsonObject.str(name: String) = getValue(name).jsonPrimitive.content

private fun JsonObject.int(name: String) = getValue(name).jsonPrimitive.int

private fun JsonObject.outputText() = getValue("output").jsonArray
    .flatMap { it.jsonObject.getValue("content").jsonArray }
    .joinToString("") { it.jsonObject.str("text") }
