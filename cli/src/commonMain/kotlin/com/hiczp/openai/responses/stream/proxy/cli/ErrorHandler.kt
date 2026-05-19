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
import kotlin.time.DurationUnit

private val logger = KotlinLogging.logger("ErrorHandler")

fun Application.installErrorHandler() {
    install(StatusPages) {
        exception<Throwable> { call, throwable ->
            if (throwable is CancellationException) {
                if (!currentCoroutineContext().isActive) {
                    val elapsed = call.requestElapsedMs()
                    logger.info { "Request cancelled: ${call.request.uri} (${elapsed}ms)" }
                    return@exception
                } else {
                    throw throwable
                }
            }
            logger.error { "Internal server error: ${throwable.message}" }
            val errorResponse = ResponsesApiProxy.errorResponse(
                message = throwable.message ?: "Proxy internal error",
                type = "internal_error",
                status = HttpStatusCode.InternalServerError,
            )
            call.respond(errorResponse)
            val elapsed = call.requestElapsedMs()
            logger.info { "${call.request.httpMethod.value} ${call.request.uri} ${errorResponse.status?.value ?: "<UnknownStatus>"} (${elapsed}ms)" }
        }
    }
}

private fun ApplicationCall.requestElapsedMs(): Int {
    val mark = attributes.getOrNull(RequestStartMarkKey) ?: return -1
    return mark.elapsedNow().toInt(DurationUnit.MILLISECONDS)
}
