package com.hiczp.openai.responses.stream.proxy.sniffer

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

data class Config(
    val upstreamUrl: String,
    val port: Int,
) {
    companion object {
        private const val ENV_UPSTREAM_URL = "SNIFFER_UPSTREAM_URL"
        private const val ENV_LISTEN_PORT = "SNIFFER_LISTEN_PORT"
        private const val DEFAULT_PORT = 8080

        fun fromEnvironment(): Config {
            val upstreamUrl = environment(ENV_UPSTREAM_URL)
                ?: error("Environment variable $ENV_UPSTREAM_URL is required (e.g. https://api.anthropic.com)")
            val port = environment(ENV_LISTEN_PORT)?.toIntOrNull() ?: DEFAULT_PORT
            return Config(upstreamUrl = upstreamUrl, port = port)
        }
    }

    fun logStartup() {
        logger.info { "=== Sniffer Reverse Proxy ===" }
        logger.info { "Upstream : $upstreamUrl" }
        logger.info { "Listen   : http://0.0.0.0:$port" }
    }
}
