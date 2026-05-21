package com.hiczp.openai.stream.proxy

import io.ktor.sse.*
import kotlinx.serialization.json.JsonObject

interface SseAccumulator {
    val response: JsonObject

    fun accumulate(event: ServerSentEvent)
}
