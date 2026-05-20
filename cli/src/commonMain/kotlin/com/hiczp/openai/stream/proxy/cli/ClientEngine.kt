package com.hiczp.openai.stream.proxy.cli

import io.ktor.client.engine.*

expect fun createClientEngine(): HttpClientEngine
