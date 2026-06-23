package com.yasashny.raft.model

import com.yasashny.raft.json.Json
import com.yasashny.raft.json.bool
import com.yasashny.raft.json.int
import com.yasashny.raft.json.intOr
import com.yasashny.raft.json.list
import com.yasashny.raft.json.reqStr
import com.yasashny.raft.json.str

sealed interface Message {
    val type: String
    fun toMap(): Map<String, Any?>
    fun encode(): String = Json.encode(toMap())
}

sealed interface PeerMessage : Message {
    val source: String
}

data class RequestVote(
    val term: Int,
    val candidateId: String,
    val lastLogIndex: Int,
    val lastLogTerm: Int,
) : PeerMessage {
    override val type get() = TYPE
    override val source get() = candidateId
    override fun toMap() = linkedMapOf<String, Any?>(
        "type" to TYPE, "term" to term, "candidateId" to candidateId,
        "lastLogIndex" to lastLogIndex, "lastLogTerm" to lastLogTerm,
    )

    companion object {
        const val TYPE = "REQUEST_VOTE"
        fun fromMap(m: Map<String, Any?>) = RequestVote(
            term = m.int("term"), candidateId = m.reqStr("candidateId"),
            lastLogIndex = m.int("lastLogIndex"), lastLogTerm = m.int("lastLogTerm"),
        )
    }
}

data class RequestVoteResp(
    val nodeId: String,
    val term: Int,
    val voteGranted: Boolean,
) : PeerMessage {
    override val type get() = TYPE
    override val source get() = nodeId
    override fun toMap() = linkedMapOf<String, Any?>(
        "type" to TYPE, "nodeId" to nodeId, "term" to term, "voteGranted" to voteGranted,
    )

    companion object {
        const val TYPE = "REQUEST_VOTE_RESP"
        fun fromMap(m: Map<String, Any?>) =
            RequestVoteResp(nodeId = m.reqStr("nodeId"), term = m.int("term"), voteGranted = m.bool("voteGranted"))
    }
}

data class AppendEntries(
    val term: Int,
    val leaderId: String,
    val prefixLen: Int,
    val prefixTerm: Int,
    val leaderCommit: Int,
    val suffix: List<LogEntry>,
) : PeerMessage {
    override val type get() = TYPE
    override val source get() = leaderId
    override fun toMap() = linkedMapOf<String, Any?>(
        "type" to TYPE, "term" to term, "leaderId" to leaderId,
        "prefixLen" to prefixLen, "prefixTerm" to prefixTerm, "leaderCommit" to leaderCommit,
        "suffix" to suffix.map { it.toMap() },
    )

    companion object {
        const val TYPE = "APPEND_ENTRIES"
        @Suppress("UNCHECKED_CAST")
        fun fromMap(m: Map<String, Any?>) = AppendEntries(
            term = m.int("term"), leaderId = m.reqStr("leaderId"),
            prefixLen = m.int("prefixLen"), prefixTerm = m.int("prefixTerm"),
            leaderCommit = m.int("leaderCommit"),
            suffix = m.list("suffix").map { LogEntry.fromMap(it as Map<String, Any?>) },
        )
    }
}

data class AppendEntriesResp(
    val nodeId: String,
    val term: Int,
    val ack: Int,
    val success: Boolean,
) : PeerMessage {
    override val type get() = TYPE
    override val source get() = nodeId
    override fun toMap() = linkedMapOf<String, Any?>(
        "type" to TYPE, "nodeId" to nodeId, "term" to term, "ack" to ack, "success" to success,
    )

    companion object {
        const val TYPE = "APPEND_ENTRIES_RESP"
        fun fromMap(m: Map<String, Any?>) = AppendEntriesResp(
            nodeId = m.reqStr("nodeId"), term = m.int("term"), ack = m.int("ack"), success = m.bool("success"),
        )
    }
}

data class ClientPut(val requestId: String, val key: String, val value: String) : Message {
    override val type get() = TYPE
    override fun toMap() =
        linkedMapOf<String, Any?>("type" to TYPE, "requestId" to requestId, "key" to key, "value" to value)

    companion object {
        const val TYPE = "CLIENT_PUT"
        fun fromMap(m: Map<String, Any?>) =
            ClientPut(requestId = m.reqStr("requestId"), key = m.reqStr("key"), value = m.reqStr("value"))
    }
}

data class ClientGet(val requestId: String, val key: String) : Message {
    override val type get() = TYPE
    override fun toMap() = linkedMapOf<String, Any?>("type" to TYPE, "requestId" to requestId, "key" to key)

    companion object {
        const val TYPE = "CLIENT_GET"
        fun fromMap(m: Map<String, Any?>) = ClientGet(requestId = m.reqStr("requestId"), key = m.reqStr("key"))
    }
}

data class LeaderQuery(val requestId: String) : Message {
    override val type get() = TYPE
    override fun toMap() = linkedMapOf<String, Any?>("type" to TYPE, "requestId" to requestId)

    companion object {
        const val TYPE = "LEADER_QUERY"
        fun fromMap(m: Map<String, Any?>) = LeaderQuery(m.reqStr("requestId"))
    }
}

data class StatusQuery(val requestId: String) : Message {
    override val type get() = TYPE
    override fun toMap() = linkedMapOf<String, Any?>("type" to TYPE, "requestId" to requestId)

    companion object {
        const val TYPE = "STATUS_QUERY"
        fun fromMap(m: Map<String, Any?>) = StatusQuery(m.reqStr("requestId"))
    }
}

data class LogDumpQuery(val requestId: String) : Message {
    override val type get() = TYPE
    override fun toMap() = linkedMapOf<String, Any?>("type" to TYPE, "requestId" to requestId)

    companion object {
        const val TYPE = "LOG_DUMP"
        fun fromMap(m: Map<String, Any?>) = LogDumpQuery(m.reqStr("requestId"))
    }
}

data class Kill(val requestId: String) : Message {
    override val type get() = TYPE
    override fun toMap() = linkedMapOf<String, Any?>("type" to TYPE, "requestId" to requestId)

    companion object {
        const val TYPE = "KILL"
        fun fromMap(m: Map<String, Any?>) = Kill(m.reqStr("requestId"))
    }
}

data class SetPartition(val requestId: String, val blocked: List<String>) : Message {
    override val type get() = TYPE
    override fun toMap() = linkedMapOf<String, Any?>("type" to TYPE, "requestId" to requestId, "blocked" to blocked)

    companion object {
        const val TYPE = "SET_PARTITION"
        fun fromMap(m: Map<String, Any?>) =
            SetPartition(m.reqStr("requestId"), m.list("blocked").map { it as String })
    }
}

data class Heal(val requestId: String) : Message {
    override val type get() = TYPE
    override fun toMap() = linkedMapOf<String, Any?>("type" to TYPE, "requestId" to requestId)

    companion object {
        const val TYPE = "HEAL"
        fun fromMap(m: Map<String, Any?>) = Heal(m.reqStr("requestId"))
    }
}

data class SetDelay(val requestId: String, val delayMs: Int) : Message {
    override val type get() = TYPE
    override fun toMap() = linkedMapOf<String, Any?>("type" to TYPE, "requestId" to requestId, "delayMs" to delayMs)

    companion object {
        const val TYPE = "SET_DELAY"
        fun fromMap(m: Map<String, Any?>) = SetDelay(m.reqStr("requestId"), m.int("delayMs"))
    }
}

data class SetElectionTimeout(val requestId: String, val minMs: Int, val maxMs: Int) : Message {
    override val type get() = TYPE
    override fun toMap() =
        linkedMapOf<String, Any?>("type" to TYPE, "requestId" to requestId, "minMs" to minMs, "maxMs" to maxMs)

    companion object {
        const val TYPE = "SET_ELECTION_TIMEOUT"
        fun fromMap(m: Map<String, Any?>) = SetElectionTimeout(m.reqStr("requestId"), m.int("minMs"), m.int("maxMs"))
    }
}

data class ClientResponse(
    val requestId: String,
    val status: String,
    val value: String? = null,
    val error: String? = null,
    val leaderHint: String? = null,
    val committedIndex: Int? = null,
) : Message {
    override val type get() = TYPE
    override fun toMap(): Map<String, Any?> {
        val m = linkedMapOf<String, Any?>("type" to TYPE, "requestId" to requestId, "status" to status)
        value?.let { m["value"] = it }
        error?.let { m["error"] = it }
        leaderHint?.let { m["leaderHint"] = it }
        committedIndex?.let { m["committedIndex"] = it }
        return m
    }

    companion object {
        const val TYPE = "CLIENT_RESP"
        fun ok(requestId: String, value: String? = null, committedIndex: Int? = null) =
            ClientResponse(requestId, "OK", value = value, committedIndex = committedIndex)

        fun error(requestId: String, code: String, leaderHint: String? = null) =
            ClientResponse(requestId, "ERROR", error = code, leaderHint = leaderHint)

        fun fromMap(m: Map<String, Any?>) = ClientResponse(
            requestId = m.reqStr("requestId"), status = m.reqStr("status"),
            value = m.str("value"), error = m.str("error"), leaderHint = m.str("leaderHint"),
            committedIndex = (m["committedIndex"] as Number?)?.toInt(),
        )
    }
}

data class LeaderInfo(
    val requestId: String,
    val nodeId: String,
    val isLeader: Boolean,
    val leaderId: String?,
    val term: Int,
) : Message {
    override val type get() = TYPE
    override fun toMap() = linkedMapOf<String, Any?>(
        "type" to TYPE, "requestId" to requestId, "nodeId" to nodeId,
        "isLeader" to isLeader, "leaderId" to leaderId, "term" to term,
    )

    companion object {
        const val TYPE = "LEADER_INFO"
        fun fromMap(m: Map<String, Any?>) = LeaderInfo(
            requestId = m.reqStr("requestId"), nodeId = m.reqStr("nodeId"),
            isLeader = m.bool("isLeader"), leaderId = m.str("leaderId"), term = m.int("term"),
        )
    }
}

data class StatusInfo(
    val requestId: String,
    val nodeId: String,
    val role: String,
    val term: Int,
    val commitIndex: Int,
    val logLength: Int,
) : Message {
    override val type get() = TYPE
    override fun toMap() = linkedMapOf<String, Any?>(
        "type" to TYPE, "requestId" to requestId, "nodeId" to nodeId, "role" to role,
        "term" to term, "commitIndex" to commitIndex, "logLength" to logLength,
    )

    companion object {
        const val TYPE = "STATUS_INFO"
        fun fromMap(m: Map<String, Any?>) = StatusInfo(
            requestId = m.reqStr("requestId"), nodeId = m.reqStr("nodeId"), role = m.reqStr("role"),
            term = m.int("term"), commitIndex = m.int("commitIndex"), logLength = m.int("logLength"),
        )
    }
}

data class LogDumpResp(
    val requestId: String,
    val nodeId: String,
    val entries: List<LogEntry>,
    val commitIndex: Int,
) : Message {
    override val type get() = TYPE
    override fun toMap() = linkedMapOf<String, Any?>(
        "type" to TYPE, "requestId" to requestId, "nodeId" to nodeId,
        "commitIndex" to commitIndex, "entries" to entries.map { it.toMap() },
    )

    companion object {
        const val TYPE = "LOG_DUMP_RESP"
        @Suppress("UNCHECKED_CAST")
        fun fromMap(m: Map<String, Any?>) = LogDumpResp(
            requestId = m.reqStr("requestId"), nodeId = m.reqStr("nodeId"),
            commitIndex = m.intOr("commitIndex", 0),
            entries = m.list("entries").map { LogEntry.fromMap(it as Map<String, Any?>) },
        )
    }
}

object ErrorCode {
    const val NOT_LEADER = "NOT_LEADER"
    const val NO_LEADER = "NO_LEADER"
    const val TIMEOUT = "TIMEOUT"
    const val BAD_REQUEST = "BAD_REQUEST"
    const val UNKNOWN_NODE = "UNKNOWN_NODE"
}

@Suppress("UNCHECKED_CAST")
fun decodeMessage(text: String): Message {
    val map = Json.parse(text) as Map<String, Any?>
    return when (val t = map["type"] as String?) {
        RequestVote.TYPE -> RequestVote.fromMap(map)
        RequestVoteResp.TYPE -> RequestVoteResp.fromMap(map)
        AppendEntries.TYPE -> AppendEntries.fromMap(map)
        AppendEntriesResp.TYPE -> AppendEntriesResp.fromMap(map)
        ClientPut.TYPE -> ClientPut.fromMap(map)
        ClientGet.TYPE -> ClientGet.fromMap(map)
        LeaderQuery.TYPE -> LeaderQuery.fromMap(map)
        StatusQuery.TYPE -> StatusQuery.fromMap(map)
        LogDumpQuery.TYPE -> LogDumpQuery.fromMap(map)
        Kill.TYPE -> Kill.fromMap(map)
        SetPartition.TYPE -> SetPartition.fromMap(map)
        Heal.TYPE -> Heal.fromMap(map)
        SetDelay.TYPE -> SetDelay.fromMap(map)
        SetElectionTimeout.TYPE -> SetElectionTimeout.fromMap(map)
        ClientResponse.TYPE -> ClientResponse.fromMap(map)
        LeaderInfo.TYPE -> LeaderInfo.fromMap(map)
        StatusInfo.TYPE -> StatusInfo.fromMap(map)
        LogDumpResp.TYPE -> LogDumpResp.fromMap(map)
        else -> throw IllegalArgumentException("unknown message type: $t")
    }
}
