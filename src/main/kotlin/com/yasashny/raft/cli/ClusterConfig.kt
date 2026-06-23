package com.yasashny.raft.cli

import com.yasashny.raft.net.Address

class ClusterConfig(
    val nodes: LinkedHashMap<String, Address>,
    var dataDirRoot: String,
    var fsync: Boolean,
    var electionMinMs: Long,
    var electionMaxMs: Long,
    val heartbeatMs: Long = 50,
    var verbose: Boolean = true,
) {
    fun peersArg(): String =
        nodes.entries.joinToString(",") { "${it.key}=${it.value.host}:${it.value.port}" }

    fun launchCommand(nodeId: String): List<String> {
        val javaBin = System.getProperty("java.home") + "/bin/java"
        val classpath = System.getProperty("java.class.path")
        val addr = nodes.getValue(nodeId)
        return listOf(
            javaBin, "-cp", classpath, NODE_MAIN_CLASS,
            "--id", nodeId,
            "--port", addr.port.toString(),
            "--data-dir", dataDirRoot,
            "--fsync", if (fsync) "on" else "off",
            "--peers", peersArg(),
            "--election-min", electionMinMs.toString(),
            "--election-max", electionMaxMs.toString(),
            "--heartbeat", heartbeatMs.toString(),
            "--verbose", if (verbose) "on" else "off",
        )
    }

    companion object {
        const val NODE_MAIN_CLASS = "com.yasashny.raft.NodeMainKt"

        fun default(dataDirRoot: String = "./data"): ClusterConfig {
            val nodes = LinkedHashMap<String, Address>()
            for (i in 1..5) nodes["n$i"] = Address("127.0.0.1", 9000 + i)
            return ClusterConfig(
                nodes = nodes,
                dataDirRoot = dataDirRoot,
                fsync = true,
                electionMinMs = 150,
                electionMaxMs = 300,
            )
        }
    }
}
