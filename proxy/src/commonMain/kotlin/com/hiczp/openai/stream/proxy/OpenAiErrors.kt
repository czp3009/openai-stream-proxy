package com.hiczp.openai.stream.proxy

import io.ktor.http.*
import io.ktor.http.content.*
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

object OpenAiErrors {
    /**
     * Builds the JSON body bytes for an OpenAI-compatible error envelope.
     *
     * @param message the human-readable error message (placed in `error.message`).
     * @param type the error type (placed in `error.type`).
     * @param param the error param (placed in `error.param`). Defaults to `""`.
     * @param code the error code (placed in `error.code`). Defaults to [type].
     * @return a JSON byte array with an OpenAI error envelope.
     */
    fun errorResponseBody(
        message: String,
        type: String,
        param: String = "",
        code: String = type,
    ): ByteArray {
        val body = JsonObject(
            mapOf(
                "error" to JsonObject(
                    mapOf(
                        "message" to JsonPrimitive(message),
                        "type" to JsonPrimitive(type),
                        "param" to JsonPrimitive(param),
                        "code" to JsonPrimitive(code),
                    )
                )
            )
        )
        return body.toString().encodeToByteArray()
    }

    /**
     * Builds an OpenAI-compatible error response.
     *
     * @param message the human-readable error message (placed in `error.message`).
     * @param type the error type (placed in `error.type`).
     * @param param the error param (placed in `error.param`). Defaults to `""`.
     * @param code the error code (placed in `error.code`). Defaults to [type].
     * @param status the HTTP status code for the response. Defaults to `502 Bad Gateway`.
     * @return a [ByteArrayContent] with content type `application/json`
     *   and an error object following the OpenAI error envelope format.
     */
    fun errorResponse(
        message: String,
        type: String,
        param: String = "",
        code: String = type,
        status: HttpStatusCode = HttpStatusCode.BadGateway,
    ): ByteArrayContent {
        return ByteArrayContent(
            bytes = errorResponseBody(
                message = message,
                type = type,
                param = param,
                code = code,
            ),
            contentType = ContentType.Application.Json,
            status = status,
        )
    }
}
