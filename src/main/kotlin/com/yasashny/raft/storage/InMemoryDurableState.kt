package com.yasashny.raft.storage

import com.yasashny.raft.model.LogEntry

class InMemoryDurableState(
    private var currentTerm: Int = 0,
    private var votedFor: String? = null,
    initialLog: List<LogEntry> = emptyList(),
) : DurableState {
    private val log = ArrayList(initialLog)

    var saveMetaCount = 0; private set
    var appendCount = 0; private set
    var truncateCount = 0; private set

    override fun load(): PersistedState = PersistedState(currentTerm, votedFor, ArrayList(log))

    override fun saveMeta(currentTerm: Int, votedFor: String?) {
        this.currentTerm = currentTerm
        this.votedFor = votedFor
        saveMetaCount++
    }

    override fun appendLog(entries: List<LogEntry>) {
        log.addAll(entries)
        appendCount++
    }

    override fun truncateLog(newLength: Int) {
        while (log.size > newLength) log.removeAt(log.size - 1)
        truncateCount++
    }

    override fun close() {}

    fun snapshot(): PersistedState = PersistedState(currentTerm, votedFor, ArrayList(log))
}
