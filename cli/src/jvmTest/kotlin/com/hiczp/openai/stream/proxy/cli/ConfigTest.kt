package com.hiczp.openai.stream.proxy.cli

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlinx.serialization.json.putJsonArray
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ConfigTest {
    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun `parses config with multiple rules`() {
        val text = buildJsonObject {
            putJsonArray("rules") {
                add(buildJsonObject { put("listenPort", 8080); put("upstreamUrl", "http://a.com") })
                add(buildJsonObject { put("listenPort", 8081); put("upstreamUrl", "http://b.com") })
            }
        }.toString()
        val config = json.decodeFromString<ProxyConfig>(text)
        assertEquals(2, config.rules.size)
        assertEquals(8080, config.rules[0].listenPort)
        assertEquals("http://a.com", config.rules[0].upstreamUrl)
        assertEquals(8081, config.rules[1].listenPort)
        assertEquals("http://b.com", config.rules[1].upstreamUrl)
        assertEquals(600L, config.timeoutSeconds)
    }

    @Test
    fun `parses config with custom timeout`() {
        val text = buildJsonObject {
            put("timeoutSeconds", 300)
            putJsonArray("rules") {
                add(buildJsonObject { put("listenPort", 8080); put("upstreamUrl", "http://a.com") })
            }
        }.toString()
        val config = json.decodeFromString<ProxyConfig>(text)
        assertEquals(300L, config.timeoutSeconds)
        assertEquals(1, config.rules.size)
    }

    @Test
    fun `parses config with empty rules`() {
        val config = json.decodeFromString<ProxyConfig>(buildJsonObject { putJsonArray("rules") {} }.toString())
        assertTrue(config.rules.isEmpty())
    }

    @Test
    fun `throws on missing required field`() {
        assertFailsWith<Exception> {
            json.decodeFromString<ProxyConfig>(
                buildJsonObject {
                    putJsonArray("rules") {
                        add(buildJsonObject {
                            put(
                                "listenPort",
                                8080
                            )
                        })
                    }
                }.toString()
            )
        }
    }

    @Test
    fun `throws when file does not exist`() {
        assertFailsWith<Exception> {
            loadConfig("nonexistent_config_file_${System.nanoTime()}.json")
        }
    }

    @Test
    fun `throws on malformed json`() {
        assertFailsWith<Exception> {
            json.decodeFromString<ProxyConfig>("not json")
        }
    }
}
