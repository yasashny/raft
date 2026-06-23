package com.yasashny.raft.cli

import com.yasashny.raft.model.StatusQuery
import java.io.File

class NodeProcessManager(
    private val config: ClusterConfig,
    private val client: ClusterClient,
) {
    private val processes = LinkedHashMap<String, Process>()

    fun startNode(id: String, silent: Boolean = false) {
        if (!config.nodes.containsKey(id)) { println("unknown node $id"); return }
        if (client.oneShot(id, StatusQuery("probe")) != null) {
            if (!silent) println("$id already running")
            return
        }
        File(config.dataDirRoot).mkdirs()
        val logFile = File(config.dataDirRoot, "$id.log")
        val pb = ProcessBuilder(config.launchCommand(id))
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
        processes[id] = pb.start()
        if (!silent) println("started $id (pid=${processes[id]?.pid()}), logs → $logFile")
    }

    fun killNode(id: String) {
        val ok = client.kill(id)

        processes.remove(id)?.let { runCatching { it.waitFor() } }
        println(if (ok) "killed $id (System.exit; data on disk kept)" else "could not reach $id (already down?)")
    }

    fun shutdownCluster() {
        for (id in config.nodes.keys) runCatching { client.kill(id) }
        for ((_, proc) in processes) runCatching { proc.destroyForcibly() }
        processes.clear()
        Thread.sleep(400)
        println("cluster shut down")
    }

    fun wipeData() {
        if (client.liveCount() > 0) { println("refusing to wipe: some nodes are still live (shutdownCluster first)"); return }
        File(config.dataDirRoot).deleteRecursively()
        File(config.dataDirRoot).mkdirs()
        println("wiped ${config.dataDirRoot}")
    }

    fun crashLeader(leaderId: String) {
        val proc = processes.remove(leaderId)
        if (proc != null) proc.destroyForcibly() else client.kill(leaderId)
    }

    fun waitForLeader(timeoutMs: Long): String? {
        val leader = client.findLeader(timeoutMs)
        println(if (leader != null) "leader elected: $leader" else "no leader within ${timeoutMs}ms")
        return leader
    }
}
