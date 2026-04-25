package com.hiczp.openai.responses.stream.proxy.mockclient

import com.openai.client.okhttp.OpenAIOkHttpClient
import com.openai.models.responses.EasyInputMessage
import com.openai.models.responses.ResponseCreateParams
import com.openai.models.responses.ResponseInputItem
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

fun main() {
    val apiKey = System.getenv("OPENAI_API_KEY")
        ?: error("OPENAI_API_KEY environment variable is required")

    val baseUrl = System.getenv("OPENAI_BASE_URL") ?: "http://localhost:8080"
    val model = System.getenv("OPENAI_MODEL") ?: "gpt-5.3-codex"
    val prompt = System.getenv("OPENAI_PROMPT") ?: "Hello"

    logger.info { "Base URL: $baseUrl" }
    logger.info { "Model: $model" }
    logger.info { "Prompt: $prompt" }
    logger.info { "Sending non-streaming request..." }

    val client = OpenAIOkHttpClient.builder()
        .apiKey(apiKey)
        .baseUrl(baseUrl)
        .build()

    val message = EasyInputMessage.builder()
        .role(EasyInputMessage.Role.USER)
        .content(prompt)
        .build()

    val params = ResponseCreateParams.builder()
        .inputOfResponse(listOf(ResponseInputItem.ofEasyInputMessage(message)))
        .model(model)
        .build()

    val response = client.responses().create(params)

    logger.info { "Response ID: ${response.id()}" }
    logger.info { "Status: ${response.status()}" }

    val text = response.output()
        .mapNotNull { it.message().orElse(null) }
        .flatMap { it.content() }
        .mapNotNull { it.outputText().orElse(null) }
        .joinToString("") { it.text() }

    logger.info { "Response: $text" }
}
