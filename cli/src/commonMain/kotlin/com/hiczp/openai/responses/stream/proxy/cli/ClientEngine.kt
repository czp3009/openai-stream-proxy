package com.hiczp.openai.responses.stream.proxy.cli

import io.ktor.client.engine.*

expect fun createClientEngine(): HttpClientEngine
