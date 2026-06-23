package com.yasashny.raft.net

import com.yasashny.raft.model.Message

data class Address(val host: String, val port: Int)

interface Transport {
    fun send(peerId: String, message: Message)
    fun setBlockedPeers(blocked: Set<String>)
    fun setDelayMs(ms: Int)
    fun close()
}
