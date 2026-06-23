package com.yasashny.raft

import com.yasashny.raft.model.AppendEntries
import com.yasashny.raft.model.LogEntry
import com.yasashny.raft.model.RequestVote
import com.yasashny.raft.model.RequestVoteResp
import com.yasashny.raft.model.Role
import com.yasashny.raft.storage.InMemoryDurableState
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ElectionTest {
    @Test
    fun `startElection bumps term, self-votes, persists and broadcasts RequestVote`() {
        val durable = InMemoryDurableState()
        val (node, tx) = testNode(durable = durable)

        node.forceElectionTimeout()

        assertEquals(1, node.currentTerm)
        assertEquals("n1", node.votedFor)
        assertEquals(Role.CANDIDATE, node.role)
        val votes = tx.all<RequestVote>()
        assertEquals(4, votes.size)
        assertTrue(votes.all { it.term == 1 && it.candidateId == "n1" })

        assertEquals("n1", durable.snapshot().votedFor)
        assertEquals(1, durable.snapshot().currentTerm)
    }

    @Test
    fun `grants vote once per term and rejects a second candidate`() {
        val (node, tx) = testNode()

        node.deliver(RequestVote(term = 1, candidateId = "n2", lastLogIndex = 0, lastLogTerm = 0))
        val first = tx.to<RequestVoteResp>("n2").single()
        assertTrue(first.voteGranted)
        assertEquals("n2", node.votedFor)

        node.deliver(RequestVote(term = 1, candidateId = "n3", lastLogIndex = 0, lastLogTerm = 0))
        val second = tx.to<RequestVoteResp>("n3").single()
        assertFalse(second.voteGranted)
    }

    @Test
    fun `rejects a candidate whose log is less up-to-date`() {
        val (node, tx) = testNode()

        node.deliver(AppendEntries(2, "n5", 0, 0, 0, listOf(LogEntry(2, "put a 1"))))
        tx.clear()

        node.deliver(RequestVote(term = 5, candidateId = "n2", lastLogIndex = 5, lastLogTerm = 1))

        val resp = tx.to<RequestVoteResp>("n2").single()
        assertFalse(resp.voteGranted)
        assertEquals(5, node.currentTerm)
        assertEquals(Role.FOLLOWER, node.role)
    }

    @Test
    fun `grants vote to a candidate with an equal or fresher log`() {
        val (node, tx) = testNode()
        node.deliver(AppendEntries(2, "n5", 0, 0, 0, listOf(LogEntry(2, "put a 1"))))
        tx.clear()

        node.deliver(RequestVote(term = 3, candidateId = "n2", lastLogIndex = 1, lastLogTerm = 2))
        assertTrue(tx.to<RequestVoteResp>("n2").single().voteGranted)
    }

    @Test
    fun `becomes leader on majority and immediately heartbeats`() {
        val (node, tx) = testNode()
        node.forceElectionTimeout()
        tx.clear()

        node.deliver(RequestVoteResp("n2", 1, true))
        assertEquals(Role.CANDIDATE, node.role)
        node.deliver(RequestVoteResp("n3", 1, true))

        assertEquals(Role.LEADER, node.role)
        val heartbeats = tx.all<AppendEntries>()
        assertEquals(4, heartbeats.size)
        assertTrue(heartbeats.all { it.term == 1 && it.leaderId == "n1" })
    }

    @Test
    fun `a higher term in a vote response forces step-down`() {
        val (node, _) = testNode()
        node.forceElectionTimeout()
        node.deliver(RequestVoteResp("n2", 9, false))
        assertEquals(9, node.currentTerm)
        assertEquals(Role.FOLLOWER, node.role)
    }

    @Test
    fun `split vote retry increments the term`() {
        val (node, _) = testNode()
        node.forceElectionTimeout()
        assertEquals(1, node.currentTerm)
        node.forceElectionTimeout()
        assertEquals(2, node.currentTerm)
        assertEquals(Role.CANDIDATE, node.role)
    }
}
