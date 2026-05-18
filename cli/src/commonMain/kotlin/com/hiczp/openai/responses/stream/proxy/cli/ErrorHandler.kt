package com.hiczp.openai.responses.stream.proxy.cli

import com.hiczp.openai.responses.stream.proxy.ResponsesApiProxy
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.response.*

fun Application.installErrorHandler() {
    install(StatusPages) {
        exception<Throwable> { call, throwable ->
            call.respond(
                ResponsesApiProxy.errorResponse(
                    message = throwable.message ?: "Proxy internal error",
                    type = "internal_error",
                    status = HttpStatusCode.InternalServerError,
                )
            )
        }
    }
}
