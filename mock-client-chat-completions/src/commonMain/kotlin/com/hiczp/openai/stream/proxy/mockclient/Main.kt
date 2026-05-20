package com.hiczp.openai.stream.proxy.mockclient

import com.openai.client.okhttp.OpenAIOkHttpClient
import com.openai.models.chat.completions.ChatCompletionCreateParams
import com.openai.models.chat.completions.ChatCompletionMessageParam
import com.openai.models.chat.completions.ChatCompletionUserMessageParam
import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

fun main() {
    val apiKey = System.getenv("OPENAI_API_KEY")
        ?: error("OPENAI_API_KEY environment variable is required")

    val baseUrl = System.getenv("OPENAI_BASE_URL") ?: "http://localhost:8080/v1"
    val model = System.getenv("OPENAI_MODEL") ?: "gpt-5.3-codex"
    val prompt = System.getenv("OPENAI_PROMPT") ?: "Hello"
    val stream = System.getenv("OPENAI_STREAM")?.toBooleanStrictOrNull() ?: false

    logger.info { "Base URL: $baseUrl" }
    logger.info { "Model: $model" }
    logger.info { "Prompt: $prompt" }
    logger.info { "Stream: $stream" }

    val client = OpenAIOkHttpClient.builder()
        .apiKey(apiKey)
        .baseUrl(baseUrl)
        .build()

    val message = ChatCompletionUserMessageParam.builder()
        .content(prompt)
        .build()

    val params = ChatCompletionCreateParams.builder()
        .messages(listOf(ChatCompletionMessageParam.ofUser(message)))
        .model(model)
        .build()

    if (stream) {
        logger.info { "Sending streaming request..." }
        client.chat().completions().createStreaming(params).use { streamResponse ->
            streamResponse.stream().forEach { chunk ->
                chunk.choices().forEach { choice ->
                    choice.delta().content().ifPresent { content ->
                        print(content)
                    }
                }
            }
        }
        println()
    } else {
        logger.info { "Sending non-streaming request..." }
        val completion = client.chat().completions().create(params)

        logger.info { "Response ID: ${completion.id()}" }

        val text = completion.choices().firstOrNull()?.message()?.content() ?: ""
        println(text)
    }
}
