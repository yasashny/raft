package com.yasashny.raft

import com.yasashny.raft.net.Address

internal fun parseFlags(args: Array<String>): Map<String, String> {
    val map = LinkedHashMap<String, String>()
    var i = 0
    while (i < args.size) {
        val token = args[i]
        if (token.startsWith("--")) {
            val key = token.removePrefix("--")
            val value = if (i + 1 < args.size && !args[i + 1].startsWith("--")) args[++i] else "true"
            map[key] = value
        }
        i++
    }
    return map
}

internal fun parsePeers(spec: String, exclude: String? = null): LinkedHashMap<String, Address> {
    val map = LinkedHashMap<String, Address>()
    for (part in spec.split(",")) {
        if (part.isBlank()) continue
        val (id, hostPort) = part.split("=", limit = 2)
        if (id == exclude) continue
        val (host, port) = hostPort.split(":", limit = 2)
        map[id] = Address(host, port.toInt())
    }
    return map
}
