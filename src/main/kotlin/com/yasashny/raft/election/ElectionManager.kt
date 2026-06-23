package com.yasashny.raft.election

import com.yasashny.raft.model.RequestVote
import com.yasashny.raft.model.RequestVoteResp
import com.yasashny.raft.model.Role
import com.yasashny.raft.node.RaftNode

class ElectionManager(private val node: RaftNode) {
    fun startElection() {
        node.currentTerm += 1
        node.votedFor = node.config.id
        node.persistMeta()
        node.role = Role.CANDIDATE
        node.currentLeader = null
        node.votesGranted.clear()
        node.votesGranted.add(node.config.id)
        node.resetElectionTimeout()
        node.logState(
            "start election → RequestVote(term=${node.currentTerm}, " +
                "lastLogIdx=${node.lastLogIndex()}, lastLogTerm=${node.lastLogTerm()}) to ${node.config.peers.keys}"
        )
        node.broadcast(
            RequestVote(
                term = node.currentTerm,
                candidateId = node.config.id,
                lastLogIndex = node.lastLogIndex(),
                lastLogTerm = node.lastLogTerm(),
            )
        )
    }

    fun onRequestVote(msg: RequestVote) {
        node.stepDownIfHigher(msg.term)

        val myLastTerm = node.lastLogTerm()
        val myLastIndex = node.lastLogIndex()
        val logOk = msg.lastLogTerm > myLastTerm ||
            (msg.lastLogTerm == myLastTerm && msg.lastLogIndex >= myLastIndex)

        val grant = msg.term == node.currentTerm && logOk &&
            (node.votedFor == null || node.votedFor == msg.candidateId)

        if (grant) {
            node.votedFor = msg.candidateId
            node.persistMeta()
            node.resetElectionTimeout()
            node.send(msg.candidateId, RequestVoteResp(node.config.id, node.currentTerm, true))
            node.logState("← RequestVote from ${msg.candidateId} (term=${msg.term}, " +
                "lastLog=${msg.lastLogIndex}/${msg.lastLogTerm}) → GRANT (votedFor persisted)")
        } else {
            val reason = when {
                msg.term < node.currentTerm -> "stale term ${msg.term} < ${node.currentTerm}"
                node.votedFor != null && node.votedFor != msg.candidateId -> "already voted for ${node.votedFor}"
                else -> "log not up-to-date (mine=$myLastIndex/$myLastTerm)"
            }
            node.send(msg.candidateId, RequestVoteResp(node.config.id, node.currentTerm, false))
            node.logState("← RequestVote from ${msg.candidateId} (term=${msg.term}) → DENY ($reason)")
        }
    }

    fun onRequestVoteResponse(msg: RequestVoteResp) {
        if (node.stepDownIfHigher(msg.term)) return
        if (node.role != Role.CANDIDATE || msg.term != node.currentTerm || !msg.voteGranted) return

        node.votesGranted.add(msg.nodeId)
        node.logState("← vote from ${msg.nodeId}: GRANTED (tally=${node.votesGranted.size}/${node.majority()})")
        if (node.votesGranted.size >= node.majority()) becomeLeader()
    }

    private fun becomeLeader() {
        node.role = Role.LEADER
        node.currentLeader = node.config.id
        for (peer in node.config.peers.keys) {
            node.sentLength[peer] = node.log.size
            node.ackedLength[peer] = 0
        }
        node.ackedLength[node.config.id] = node.log.size
        node.logState("WON election → LEADER (votes=${node.votesGranted})")
        node.onElectedLeader()
    }
}
