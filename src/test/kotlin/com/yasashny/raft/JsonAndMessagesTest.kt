package com.yasashny.raft

import com.yasashny.raft.json.Json
import com.yasashny.raft.model.AppendEntries
import com.yasashny.raft.model.AppendEntriesResp
import com.yasashny.raft.model.ClientPut
import com.yasashny.raft.model.LogEntry
import com.yasashny.raft.model.RequestVote
import com.yasashny.raft.model.decodeMessage
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class JsonAndMessagesTest {
    @Test
    fun `json round trips nested structures`() {
        val value = linkedMapOf(
            "type" to "X", "n" to 47L, "ok" to true, "nil" to null,
            "list" to listOf(1L, 2L, 3L),
            "obj" to linkedMapOf("k" to "v with spaces"),
        )
        @Suppress("UNCHECKED_CAST")
        val parsed = Json.parse(Json.encode(value)) as Map<String, Any?>
        assertEquals("X", parsed["type"])
        assertEquals(47L, parsed["n"])
        assertEquals(true, parsed["ok"])
        assertEquals(null, parsed["nil"])
        assertEquals(listOf(1L, 2L, 3L), parsed["list"])
    }

    @Test
    fun `json escapes special characters in strings`() {
        val s = "quote \" backslash \\ newline \n tab \t end"
        val parsed = Json.parse(Json.encode(s))
        assertEquals(s, parsed)
    }

    @Test
    fun `AppendEntries with suffix round trips through the wire`() {
        val msg = AppendEntries(
            term = 3, leaderId = "n1", prefixLen = 4, prefixTerm = 3, leaderCommit = 4,
            suffix = listOf(LogEntry(3, "put x 8"), LogEntry(3, "put y 9")),
        )
        val decoded = decodeMessage(msg.encode()) as AppendEntries
        assertEquals(msg, decoded)
    }

    @Test
    fun `consensus messages match the spec field names`() {
        val rv = RequestVote(3, "n4", 47, 2)
        assertTrue(rv.encode().contains("\"type\":\"REQUEST_VOTE\""))
        assertTrue(rv.encode().contains("\"candidateId\":\"n4\""))
        assertTrue(rv.encode().contains("\"lastLogIndex\":47"))

        val resp = AppendEntriesResp("n2", 3, 5, true)
        val decoded = decodeMessage(resp.encode()) as AppendEntriesResp
        assertEquals(resp, decoded)

        val put = decodeMessage(ClientPut("r1", "x", "8").encode()) as ClientPut
        assertEquals("x", put.key); assertEquals("8", put.value)
    }
}
