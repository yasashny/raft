package com.yasashny.raft

import com.yasashny.raft.model.AppendEntries
import com.yasashny.raft.model.AppendEntriesResp
import com.yasashny.raft.model.ClientPut
import com.yasashny.raft.model.ClientResponse
import com.yasashny.raft.model.LogEntry
import com.yasashny.raft.model.Role
import com.yasashny.raft.node.RaftNode
import com.yasashny.raft.storage.InMemoryDurableState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ReplicationTest {
    @Test
    fun `follower appends entries, persists, and acks`() {
        val durable = InMemoryDurableState()
        val (node, tx) = testNode(durable = durable)

        node.deliver(
            AppendEntries(1, "n2", prefixLen = 0, prefixTerm = 0, leaderCommit = 0,
                suffix = listOf(LogEntry(1, "put x 1"), LogEntry(1, "put y 2")))
        )

        assertEquals(2, node.log.size)
        assertEquals(2, durable.snapshot().log.size)
        val resp = tx.to<AppendEntriesResp>("n2").single()
        assertTrue(resp.success); assertEquals(2, resp.ack)
        assertEquals(Role.FOLLOWER, node.role)
    }

    @Test
    fun `follower rejects when its log is shorter than prefixLen (triggers backtrack)`() {
        val (node, tx) = testNode()
        node.deliver(AppendEntries(1, "n2", prefixLen = 3, prefixTerm = 1, leaderCommit = 0, suffix = emptyList()))
        val resp = tx.to<AppendEntriesResp>("n2").single()
        assertFalse(resp.success); assertEquals(0, resp.ack)
    }

    @Test
    fun `follower truncates a conflicting tail and appends the leader's entries`() {
        val (node, _) = testNode()

        node.deliver(AppendEntries(1, "n2", 0, 0, 0, listOf(LogEntry(1, "put a 1"), LogEntry(1, "put b 1"))))

        node.deliver(AppendEntries(2, "n2", prefixLen = 1, prefixTerm = 1, leaderCommit = 0,
            suffix = listOf(LogEntry(2, "put c 2"))))

        assertEquals(listOf(LogEntry(1, "put a 1"), LogEntry(2, "put c 2")), node.log)
    }

    @Test
    fun `follower applies only committed entries to the KV state machine`() {
        val (node, _) = testNode()
        node.deliver(
            AppendEntries(1, "n2", 0, 0, leaderCommit = 1,
                suffix = listOf(LogEntry(1, "put x 1"), LogEntry(1, "put y 2")))
        )
        assertEquals(1, node.commitIndex)
        assertEquals("1", node.kvGet("x"))
        assertEquals(null, node.kvGet("y"))
    }

    @Test
    fun `leader backtracks sentLength on failure and retries with a smaller prefix`() {
        val (node, tx) = makeLeader(term = 1, log = listOf(LogEntry(1, "a"), LogEntry(1, "b"), LogEntry(1, "c")))
        tx.clear()

        node.deliver(AppendEntriesResp("n2", term = 1, ack = 0, success = false))

        assertEquals(2, node.sentLength["n2"])
        val retry = tx.to<AppendEntries>("n2").single()
        assertEquals(2, retry.prefixLen)
    }

    @Test
    fun `leader commits by majority once the entry is from its current term`() {
        val (node, _) = makeLeader(term = 1, log = listOf(LogEntry(1, "put a 1")))
        node.deliver(AppendEntriesResp("n2", 1, ack = 1, success = true))
        node.deliver(AppendEntriesResp("n3", 1, ack = 1, success = true))
        assertEquals(1, node.commitIndex)
        assertEquals("1", node.kvGet("a"))
    }

    @Test
    fun `leader does NOT commit an old-term entry until a current-term entry reaches majority`() {
        val (node, _) = makeLeader(term = 2, log = listOf(LogEntry(1, "put a 1")))

        node.deliver(AppendEntriesResp("n2", 2, ack = 1, success = true))
        node.deliver(AppendEntriesResp("n3", 2, ack = 1, success = true))
        assertEquals(0, node.commitIndex)

        val reply = CapturingReply()
        node.deliver(ClientPut("r1", "b", "2"), reply)
        assertEquals(2, node.log.size)

        node.deliver(AppendEntriesResp("n2", 2, ack = 2, success = true))
        node.deliver(AppendEntriesResp("n3", 2, ack = 2, success = true))
        assertEquals(2, node.commitIndex)
        assertEquals("1", node.kvGet("a"))
        assertEquals("2", node.kvGet("b"))
        val ok = reply.responses.single() as ClientResponse
        assertEquals("OK", ok.status)
        assertEquals(2, ok.committedIndex)
    }

    @Test
    fun `leader redirects a client put when it is not the leader`() {
        val (node, _) = testNode()
        val reply = CapturingReply()
        node.deliver(ClientPut("r1", "x", "1"), reply)
        val resp = reply.responses.single() as ClientResponse
        assertEquals("ERROR", resp.status)
        assertEquals("NOT_LEADER", resp.error)
    }

    private fun makeLeader(term: Int, log: List<LogEntry>): Pair<RaftNode, RecordingTransport> {
        val (node, tx) = testNode()
        node.currentTerm = term
        node.role = Role.LEADER
        node.log.addAll(log)
        node.ackedLength["n1"] = log.size
        for (peer in listOf("n2", "n3", "n4", "n5")) {
            node.sentLength[peer] = log.size
            node.ackedLength[peer] = 0
        }
        return node to tx
    }
}
