package com.hiczp.openai.stream.proxy.sniffer

import io.github.oshai.kotlinlogging.KotlinLogging

private val logger = KotlinLogging.logger {}

data class Config(
    val upstreamBaseUrl: String,
    val port: Int,
) {
    companion object {
        private const val ENV_UPSTREAM_BASE_URL = "UPSTREAM_BASE_URL"
        private const val DEFAULT_UPSTREAM_BASE_URL = "https://api.openai.com"
        private const val ENV_LISTEN_PORT = "LISTEN_PORT"
        private const val DEFAULT_PORT = 8080

        fun fromEnvironment(): Config {
            val upstreamBaseUrl = System.getenv(ENV_UPSTREAM_BASE_URL)?.trimEnd('/') ?: DEFAULT_UPSTREAM_BASE_URL
            val port = System.getenv(ENV_LISTEN_PORT)?.toIntOrNull() ?: DEFAULT_PORT
            return Config(upstreamBaseUrl = upstreamBaseUrl, port = port)
        }
    }

    fun logStartup() {
        logger.info { "=== Sniffer Reverse Proxy ===" }
        logger.info { "Upstream: $upstreamBaseUrl" }
        logger.info { "Listen: http://0.0.0.0:$port" }
    }
}
