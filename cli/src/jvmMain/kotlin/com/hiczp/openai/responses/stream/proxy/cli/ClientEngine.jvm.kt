package com.hiczp.openai.responses.stream.proxy.cli

import io.ktor.client.engine.*
import io.ktor.client.engine.cio.*

actual fun createClientEngine(): HttpClientEngine = CIO.create()
