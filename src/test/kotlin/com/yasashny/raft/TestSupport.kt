package com.yasashny.raft

import com.yasashny.raft.model.Message
import com.yasashny.raft.net.Address
import com.yasashny.raft.net.ReplySink
import com.yasashny.raft.net.Transport
import com.yasashny.raft.node.NodeConfig
import com.yasashny.raft.node.RaftNode
import com.yasashny.raft.storage.DurableState
import com.yasashny.raft.storage.InMemoryDurableState

class RecordingTransport : Transport {
    val sent = mutableListOf<Pair<String, Message>>()
    var blocked: Set<String> = emptySet()

    override fun send(peerId: String, message: Message) { sent.add(peerId to message) }
    override fun setBlockedPeers(blocked: Set<String>) { this.blocked = blocked }
    override fun setDelayMs(ms: Int) {}
    override fun close() {}

    inline fun <reified T : Message> to(peer: String): List<T> =
        sent.filter { it.first == peer }.mapNotNull { it.second as? T }

    inline fun <reified T : Message> all(): List<T> = sent.mapNotNull { it.second as? T }
    fun clear() = sent.clear()
}

class CapturingReply : ReplySink {
    val responses = mutableListOf<Message>()
    override fun send(message: Message) { responses.add(message) }
}

fun testNode(
    id: String = "n1",
    peers: List<String> = listOf("n2", "n3", "n4", "n5"),
    durable: DurableState = InMemoryDurableState(),
): Pair<RaftNode, RecordingTransport> {
    val peerMap = LinkedHashMap<String, Address>()
    for (p in peers) peerMap[p] = Address("127.0.0.1", 0)
    val config = NodeConfig(id, 0, peerMap, "test-data", fsync = false)
    val transport = RecordingTransport()
    val node = RaftNode(config, durable, transport, clock = { 0L }, logSink = {})
    node.loadState()
    return node to transport
}
