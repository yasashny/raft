package com.yasashny.raft

import com.yasashny.raft.model.AppendEntries
import com.yasashny.raft.model.LogEntry
import com.yasashny.raft.model.RequestVote
import com.yasashny.raft.storage.FileDurableState
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.io.path.createTempDirectory
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PersistenceTest {
    private val root: Path = createTempDirectory("raft-test")

    @AfterTest
    fun cleanup() {
        root.toFile().deleteRecursively()
    }

    @Test
    fun `meta and log survive a reopen`() {
        FileDurableState(root.toString(), "n1", fsync = true).apply {
            load()
            saveMeta(currentTerm = 7, votedFor = "n3")
            appendLog(listOf(LogEntry(7, "put a 1"), LogEntry(7, "put b 2")))
            close()
        }

        val recovered = FileDurableState(root.toString(), "n1", fsync = true).load()
        assertEquals(7, recovered.currentTerm)
        assertEquals("n3", recovered.votedFor)
        assertEquals(listOf(LogEntry(7, "put a 1"), LogEntry(7, "put b 2")), recovered.log)
    }

    @Test
    fun `truncation is persisted`() {
        FileDurableState(root.toString(), "n2", fsync = true).apply {
            load()
            appendLog(listOf(LogEntry(1, "a"), LogEntry(1, "b"), LogEntry(2, "c")))
            truncateLog(1)
            appendLog(listOf(LogEntry(3, "d")))
            close()
        }
        val recovered = FileDurableState(root.toString(), "n2", fsync = true).load()
        assertEquals(listOf(LogEntry(1, "a"), LogEntry(3, "d")), recovered.log)
    }

    @Test
    fun `a torn trailing line is dropped on recovery`() {
        FileDurableState(root.toString(), "n3", fsync = true).apply {
            load()
            appendLog(listOf(LogEntry(1, "put x 1")))
            close()
        }

        val logFile = root.resolve("n3").resolve("log")
        Files.write(logFile, "{\"term\":1,\"comma".toByteArray(), StandardOpenOption.APPEND)

        val recovered = FileDurableState(root.toString(), "n3", fsync = true).load()
        assertEquals(listOf(LogEntry(1, "put x 1")), recovered.log)
    }

    @Test
    fun `a node recovers its term, vote and log across a restart`() {
        val durable1 = FileDurableState(root.toString(), "n1", fsync = true)
        val (node1, _) = testNodeWith(durable1)

        node1.deliver(RequestVote(term = 4, candidateId = "n2", lastLogIndex = 0, lastLogTerm = 0))
        node1.deliver(AppendEntries(4, "n2", 0, 0, 1, listOf(LogEntry(4, "put k v"))))
        durable1.close()

        val durable2 = FileDurableState(root.toString(), "n1", fsync = true)
        val (node2, _) = testNodeWith(durable2)
        assertEquals(4, node2.currentTerm)
        assertEquals("n2", node2.votedFor)
        assertEquals(1, node2.log.size)
        assertEquals("put k v", node2.log[0].command)
        assertTrue(node2.commitIndex == 0)
        durable2.close()
    }

    private fun testNodeWith(durable: FileDurableState) = testNode(durable = durable)
}
