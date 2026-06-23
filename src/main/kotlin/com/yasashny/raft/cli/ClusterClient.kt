package com.yasashny.raft.cli

import com.yasashny.raft.model.ClientGet
import com.yasashny.raft.model.ClientPut
import com.yasashny.raft.model.ClientResponse
import com.yasashny.raft.model.ErrorCode
import com.yasashny.raft.model.Heal
import com.yasashny.raft.model.Kill
import com.yasashny.raft.model.LeaderInfo
import com.yasashny.raft.model.LeaderQuery
import com.yasashny.raft.model.LogDumpQuery
import com.yasashny.raft.model.LogDumpResp
import com.yasashny.raft.model.Message
import com.yasashny.raft.model.SetDelay
import com.yasashny.raft.model.SetElectionTimeout
import com.yasashny.raft.model.SetPartition
import com.yasashny.raft.model.StatusInfo
import com.yasashny.raft.model.StatusQuery
import com.yasashny.raft.net.Address
import java.util.UUID

class ClusterClient(private val nodes: Map<String, Address>, private val timeoutMs: Int = 3000) {
    private fun newRequestId() = UUID.randomUUID().toString()

    fun oneShot(nodeId: String, message: Message): Message? =
        runCatching { NodeClient(nodes.getValue(nodeId), timeoutMs).use { it.request(message) } }.getOrNull()

    fun findLeader(overallTimeoutMs: Long = 3000): String? {
        val deadline = System.currentTimeMillis() + overallTimeoutMs
        do {
            var best: LeaderInfo? = null
            for (id in nodes.keys) {
                val resp = oneShot(id, LeaderQuery(newRequestId())) as? LeaderInfo ?: continue
                val current = best
                if (resp.isLeader && (current == null || resp.term > current.term)) best = resp
            }
            best?.let { return it.nodeId }
            Thread.sleep(80)
        } while (System.currentTimeMillis() < deadline)
        return null
    }

    fun put(key: String, value: String, overallTimeoutMs: Long = 5000): ClientResponse {
        val reqId = newRequestId()
        val deadline = System.currentTimeMillis() + overallTimeoutMs
        var leader = findLeader(overallTimeoutMs)
        while (System.currentTimeMillis() < deadline) {
            if (leader == null) {
                leader = findLeader(500); if (leader == null) { Thread.sleep(100); continue }
            }
            try {
                val resp = NodeClient(nodes.getValue(leader), timeoutMs).use {
                    it.request(ClientPut(reqId, key, value))
                } as ClientResponse
                when {
                    resp.status == "OK" -> return resp
                    resp.error == ErrorCode.NOT_LEADER -> { leader = resp.leaderHint; Thread.sleep(50) }
                    else -> return resp
                }
            } catch (_: Exception) {
                leader = null; Thread.sleep(100)
            }
        }
        return ClientResponse.error(reqId, ErrorCode.TIMEOUT)
    }

    fun get(key: String, overallTimeoutMs: Long = 5000): ClientResponse {
        val reqId = newRequestId()
        val deadline = System.currentTimeMillis() + overallTimeoutMs
        var leader = findLeader(overallTimeoutMs)
        while (System.currentTimeMillis() < deadline) {
            if (leader == null) { leader = findLeader(500); if (leader == null) { Thread.sleep(100); continue } }
            try {
                val resp = NodeClient(nodes.getValue(leader), timeoutMs).use {
                    it.request(ClientGet(reqId, key))
                } as ClientResponse
                when {
                    resp.status == "OK" -> return resp
                    resp.error == ErrorCode.NOT_LEADER -> { leader = resp.leaderHint; Thread.sleep(50) }
                    else -> return resp
                }
            } catch (_: Exception) {
                leader = null; Thread.sleep(100)
            }
        }
        return ClientResponse.error(reqId, ErrorCode.TIMEOUT)
    }

    fun putTo(nodeId: String, key: String, value: String, timeoutMs: Int = 2000): ClientResponse {
        val reqId = newRequestId()
        return runCatching {
            NodeClient(nodes.getValue(nodeId), timeoutMs).use {
                it.request(ClientPut(reqId, key, value))
            } as ClientResponse
        }.getOrElse { ClientResponse.error(reqId, ErrorCode.TIMEOUT) }
    }

    fun status(): List<Pair<String, StatusInfo?>> =
        nodes.keys.map { id -> id to (oneShot(id, StatusQuery(newRequestId())) as? StatusInfo) }

    fun leaders(): List<LeaderInfo> =
        nodes.keys.mapNotNull { id -> oneShot(id, LeaderQuery(newRequestId())) as? LeaderInfo }

    fun logDump(nodeId: String): LogDumpResp? =
        oneShot(nodeId, LogDumpQuery(newRequestId())) as? LogDumpResp

    fun kill(nodeId: String): Boolean = oneShot(nodeId, Kill(newRequestId())) != null

    fun partition(g1: List<String>, g2: List<String>) {
        for (id in g1) oneShot(id, SetPartition(newRequestId(), g2))
        for (id in g2) oneShot(id, SetPartition(newRequestId(), g1))
    }

    fun heal() {
        for (id in nodes.keys) oneShot(id, Heal(newRequestId()))
    }

    fun setDelay(nodeId: String, ms: Int) {
        oneShot(nodeId, SetDelay(newRequestId(), ms))
    }

    fun setElectionTimeout(minMs: Int, maxMs: Int) {
        for (id in nodes.keys) oneShot(id, SetElectionTimeout(newRequestId(), minMs, maxMs))
    }

    fun liveCount(): Int = nodes.keys.count { oneShot(it, StatusQuery(newRequestId())) != null }
}
