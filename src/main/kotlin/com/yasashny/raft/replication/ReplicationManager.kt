package com.yasashny.raft.replication

import com.yasashny.raft.model.AppendEntries
import com.yasashny.raft.model.AppendEntriesResp
import com.yasashny.raft.model.ClientPut
import com.yasashny.raft.model.ClientResponse
import com.yasashny.raft.model.ErrorCode
import com.yasashny.raft.model.LogEntry
import com.yasashny.raft.model.Role
import com.yasashny.raft.net.ReplySink
import com.yasashny.raft.node.KvStateMachine
import com.yasashny.raft.node.RaftNode

class ReplicationManager(private val node: RaftNode) {
    fun replicateToAll() {
        for (peer in node.config.peers.keys) replicateLog(peer)
    }

    fun replicateLog(peer: String) {
        val prefixLen = node.sentLength[peer] ?: 0
        val suffix = ArrayList(node.log.subList(prefixLen, node.log.size))
        val prefixTerm = if (prefixLen > 0) node.log[prefixLen - 1].term else 0
        node.send(
            peer,
            AppendEntries(
                term = node.currentTerm,
                leaderId = node.config.id,
                prefixLen = prefixLen,
                prefixTerm = prefixTerm,
                leaderCommit = node.commitIndex,
                suffix = suffix,
            )
        )
        if (suffix.isNotEmpty()) {
            node.logTrace("→ AppendEntries $peer: prefixLen=$prefixLen prefixTerm=$prefixTerm " +
                "entries=${suffix.size} leaderCommit=${node.commitIndex}")
        }
    }

    fun onClientPut(msg: ClientPut, reply: ReplySink) {
        if (node.role != Role.LEADER) {
            reply.send(ClientResponse.error(msg.requestId, ErrorCode.NOT_LEADER, leaderHint = node.currentLeader))
            return
        }
        val command = KvStateMachine.putCommand(msg.key, msg.value)
        node.appendToLog(listOf(LogEntry(node.currentTerm, command)))
        node.ackedLength[node.config.id] = node.log.size
        node.registerPendingPut(node.log.size, msg.requestId, reply)
        node.logTrace("← CLIENT_PUT '$command' → log[${node.log.size}] (term ${node.currentTerm}), persisted; replicating")
        replicateToAll()
        node.deferHeartbeat()
    }

    fun onAppendEntriesResponse(msg: AppendEntriesResp) {
        if (node.stepDownIfHigher(msg.term)) return
        if (msg.term != node.currentTerm || node.role != Role.LEADER) return

        val acked = node.ackedLength[msg.nodeId] ?: 0
        if (msg.success && msg.ack >= acked) {
            node.sentLength[msg.nodeId] = msg.ack
            node.ackedLength[msg.nodeId] = msg.ack
            commitLogEntries()
        } else if ((node.sentLength[msg.nodeId] ?: 0) > 0) {
            node.sentLength[msg.nodeId] = (node.sentLength[msg.nodeId] ?: 0) - 1
            node.logTrace("← ack from ${msg.nodeId}: FAIL → backtrack sentLength=${node.sentLength[msg.nodeId]}, resend")
            replicateLog(msg.nodeId)
        }
    }

    private fun commitLogEntries() {
        val acksDescending = node.config.allNodeIds.map { node.ackedLength[it] ?: 0 }.sortedDescending()
        val ready = acksDescending[node.majority() - 1]

        if (ready > node.commitIndex && node.log[ready - 1].term == node.currentTerm) {
            node.advanceCommitIndex(ready)
            node.logTrace("commit advanced to ${node.commitIndex} (majority ack, term caveat satisfied)")
        }
    }

    fun onAppendEntries(msg: AppendEntries) {
        node.stepDownIfHigher(msg.term)

        if (msg.term == node.currentTerm) {
            node.role = Role.FOLLOWER
            node.currentLeader = msg.leaderId
            node.resetElectionTimeout()
        }

        val logOk = node.log.size >= msg.prefixLen &&
            (msg.prefixLen == 0 || node.log[msg.prefixLen - 1].term == msg.prefixTerm)

        if (msg.term == node.currentTerm && logOk) {
            appendEntriesLocal(msg.prefixLen, msg.leaderCommit, msg.suffix)
            val ack = msg.prefixLen + msg.suffix.size
            node.send(msg.leaderId, AppendEntriesResp(node.config.id, node.currentTerm, ack, success = true))
            if (msg.suffix.isNotEmpty()) {
                node.logTrace("← AppendEntries from ${msg.leaderId}: prefixLen=${msg.prefixLen} " +
                    "entries=${msg.suffix.size} leaderCommit=${msg.leaderCommit} → ACCEPT, persisted, ack=$ack")
            } else {
                node.logHeartbeatReceived(msg.leaderId, msg.leaderCommit)
            }
        } else {
            node.send(msg.leaderId, AppendEntriesResp(node.config.id, node.currentTerm, ack = 0, success = false))
            node.logTrace("← AppendEntries from ${msg.leaderId}: prefixLen=${msg.prefixLen} " +
                "prefixTerm=${msg.prefixTerm} → REJECT (logOk=false, log.len=${node.log.size}), success=false")
        }
    }

    private fun appendEntriesLocal(prefixLen: Int, leaderCommit: Int, suffix: List<LogEntry>) {
        if (suffix.isNotEmpty() && node.log.size > prefixLen) {
            val index = minOf(node.log.size, prefixLen + suffix.size) - 1
            if (node.log[index].term != suffix[index - prefixLen].term) {
                node.logState("TRUNCATE log ${node.log.size} → $prefixLen (conflicting uncommitted tail dropped)")
                node.truncateLogTo(prefixLen)
            }
        }

        if (prefixLen + suffix.size > node.log.size) {
            val from = node.log.size - prefixLen
            node.appendToLog(suffix.subList(from, suffix.size))
        }

        if (leaderCommit > node.commitIndex) {
            node.advanceCommitIndex(minOf(leaderCommit, node.log.size))
        }
    }
}
