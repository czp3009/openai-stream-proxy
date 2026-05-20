package com.hiczp.openai.stream.proxy.cli

import io.ktor.client.engine.*
import io.ktor.client.engine.curl.*

actual fun createClientEngine(): HttpClientEngine = Curl.create()
