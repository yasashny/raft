package com.yasashny.raft.node

class KvStateMachine {
    private val kv = HashMap<String, String>()

    fun apply(command: String) {
        val parts = command.split(" ", limit = 3)
        if (parts.size >= 3 && parts[0] == "put") kv[parts[1]] = parts[2]
    }

    fun get(key: String): String? = kv[key]

    fun clear() = kv.clear()

    companion object {
        fun putCommand(key: String, value: String): String = "put $key $value"
    }
}
