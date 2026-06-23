package com.yasashny.raft.net

import com.yasashny.raft.model.Message

fun interface ReplySink {
    fun send(message: Message)
}
