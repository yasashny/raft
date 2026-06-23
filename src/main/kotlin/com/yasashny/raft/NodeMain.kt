package com.yasashny.raft

import com.yasashny.raft.net.RpcServer
import com.yasashny.raft.net.TcpTransport
import com.yasashny.raft.node.NodeConfig
import com.yasashny.raft.node.RaftNode
import com.yasashny.raft.storage.FileDurableState

fun main(args: Array<String>) {
    val flags = parseFlags(args)
    val id = flags.getValue("id")
    val port = flags.getValue("port").toInt()
    val dataDir = flags.getValue("data-dir")
    val fsync = (flags["fsync"] ?: "on") == "on"
    val verbose = (flags["verbose"] ?: "on") == "on"
    val peers = parsePeers(flags.getValue("peers"), exclude = id)

    val config = NodeConfig(
        id = id,
        port = port,
        peers = peers,
        dataDirRoot = dataDir,
        fsync = fsync,
        electionMinMs = flags["election-min"]?.toLong() ?: 150L,
        electionMaxMs = flags["election-max"]?.toLong() ?: 300L,
        heartbeatMs = flags["heartbeat"]?.toLong() ?: 50L,
    )

    val durable = FileDurableState(dataDir, id, fsync)
    val transport = TcpTransport(id, peers)
    val node = RaftNode(config, durable, transport, verbose = verbose)
    val server = RpcServer(port, id, logger = { println("[$id] $it") }) { message, conn ->
        node.onInbound(message, conn)
    }

    node.start()
    server.start()
    println(
        "node $id listening on $port  fsync=$fsync  " +
            "election=${config.electionMinMs}-${config.electionMaxMs}ms  peers=${peers.keys}"
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        runCatching { server.close() }
        runCatching { node.stop() }
    })
}
