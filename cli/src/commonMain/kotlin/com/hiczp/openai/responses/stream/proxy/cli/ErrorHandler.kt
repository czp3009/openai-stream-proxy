package com.hiczp.openai.responses.stream.proxy.cli

import com.hiczp.openai.responses.stream.proxy.ResponsesApiProxy
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive

private val logger = KotlinLogging.logger {}

fun Application.installErrorHandler() {
    install(StatusPages) {
        exception<Throwable> { call, throwable ->
            if (throwable is CancellationException) {
                if (!currentCoroutineContext().isActive) {
                    logger.debug { "Request cancelled: ${call.request.uri}" }
                    return@exception
                } else {
                    throw throwable
                }
            }
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
