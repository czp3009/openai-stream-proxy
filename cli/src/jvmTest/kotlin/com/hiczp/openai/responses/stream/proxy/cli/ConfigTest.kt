package com.hiczp.openai.responses.stream.proxy.cli

import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class ConfigTest {
    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun `parses multiple rules`() {
        val text =
            """[{"listenPort":8080,"upstreamUrl":"http://a.com"},{"listenPort":8081,"upstreamUrl":"http://b.com"}]"""
        val rules = json.decodeFromString<List<ProxyRule>>(text)
        assertEquals(2, rules.size)
        assertEquals(8080, rules[0].listenPort)
        assertEquals("http://a.com", rules[0].upstreamUrl)
        assertEquals(8081, rules[1].listenPort)
        assertEquals("http://b.com", rules[1].upstreamUrl)
    }

    @Test
    fun `parses empty array`() {
        val rules = json.decodeFromString<List<ProxyRule>>("[]")
        assertTrue(rules.isEmpty())
    }

    @Test
    fun `throws on missing required field`() {
        assertFailsWith<Exception> {
            json.decodeFromString<List<ProxyRule>>("""[{"listenPort":8080}]""")
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
            json.decodeFromString<List<ProxyRule>>("not json")
        }
    }
}
