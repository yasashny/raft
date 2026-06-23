package com.yasashny.raft.storage

import com.yasashny.raft.model.LogEntry

data class PersistedState(
    val currentTerm: Int,
    val votedFor: String?,
    val log: List<LogEntry>,
)

interface DurableState {
    fun load(): PersistedState

    fun saveMeta(currentTerm: Int, votedFor: String?)

    fun appendLog(entries: List<LogEntry>)

    fun truncateLog(newLength: Int)

    fun close()
}
