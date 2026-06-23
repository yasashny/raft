package com.yasashny.raft.node

import com.yasashny.raft.net.Address

class NodeConfig(
    val id: String,
    val port: Int,
    val peers: Map<String, Address>,
    val dataDirRoot: String,
    val fsync: Boolean,
    var electionMinMs: Long = 150,
    var electionMaxMs: Long = 300,
    val heartbeatMs: Long = 50,
) {
    val clusterSize: Int get() = peers.size + 1

    val majority: Int get() = clusterSize / 2 + 1

    val allNodeIds: Set<String> get() = peers.keys + id
}
