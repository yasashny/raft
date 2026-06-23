package com.yasashny.raft.model

data class LogEntry(val term: Int, val command: String) {
    fun toMap(): Map<String, Any?> = linkedMapOf("term" to term, "command" to command)

    companion object {
        fun fromMap(m: Map<String, Any?>): LogEntry =
            LogEntry(term = (m["term"] as Number).toInt(), command = m["command"] as String)
    }
}
