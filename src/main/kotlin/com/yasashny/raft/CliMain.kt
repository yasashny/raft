package com.yasashny.raft

import com.yasashny.raft.cli.Cli
import com.yasashny.raft.cli.ClusterConfig

fun main(args: Array<String>) {
    val flags = parseFlags(args)
    val dataDir = flags["data-dir"] ?: "./data"

    val verbose = (flags["verbose"] ?: "on") == "on"
    val config = if (flags.containsKey("peers")) {
        ClusterConfig(
            nodes = parsePeers(flags.getValue("peers")),
            dataDirRoot = dataDir,
            fsync = (flags["fsync"] ?: "on") == "on",
            electionMinMs = flags["election-min"]?.toLong() ?: 150L,
            electionMaxMs = flags["election-max"]?.toLong() ?: 300L,
            verbose = verbose,
        )
    } else {
        ClusterConfig.default(dataDir).apply {
            fsync = (flags["fsync"] ?: "on") == "on"
            electionMinMs = flags["election-min"]?.toLong() ?: 150L
            electionMaxMs = flags["election-max"]?.toLong() ?: 300L
            this.verbose = verbose
        }
    }

    Cli(config).repl()
}
