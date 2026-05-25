package com.hiczp.openai.stream.proxy.cli

import com.hiczp.openai.stream.proxy.OpenAiErrors
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

private val logger = KotlinLogging.logger("com.hiczp.openai.stream.proxy.cli.ErrorHandler")

fun Application.installErrorHandler() {
    install(StatusPages) {
        exception<Throwable> { call, throwable ->
            val elapsed = call.requestElapsedMs()
            if (throwable is CancellationException) {
                if (!currentCoroutineContext().isActive) {
                    // HttpRequestLifecycle cancels the call when the downstream connection closes.
                    // The response cannot be delivered anymore, so only log and stop handling.
                    logger.info { "Request cancelled: ${call.request.uri} (${elapsed}ms)" }
                    return@exception
                } else {
                    throw throwable
                }
            }
            if (call.response.isCommitted || call.response.isSent) {
                logger.warn { "Response already started, cannot send fallback error: ${call.request.uri} (${elapsed}ms) [${throwable.message}]" }
                return@exception
            }
            logger.error { "Internal server error: ${throwable.message}" }
            val errorResponse = OpenAiErrors.errorResponse(
                message = throwable.message ?: "Proxy internal error",
                type = "internal_error",
                status = HttpStatusCode.InternalServerError,
            )
            call.respond(errorResponse)
            logger.info { "${call.request.httpMethod.value} ${call.request.uri} ${errorResponse.status?.value ?: "<UnknownStatus>"} (${elapsed}ms)" }
        }
    }
}

private fun ApplicationCall.requestElapsedMs(): Int {
    val mark = attributes.getOrNull(RequestStartMarkKey) ?: return -1
    return mark.elapsedNow().toInt(DurationUnit.MILLISECONDS)
}
