package com.yasashny.raft.cli

import java.io.File

data class BenchRow(
    val part: String,
    val scenario: String,
    val liveNodes: Int,
    val electionTimeoutMs: String,
    val fsync: String,
    val threads: Int,
    val totalOps: Int,
    val throughputOpsSec: Double,
    val avgMs: Double,
    val p50Ms: Double,
    val p95Ms: Double,
    val p99Ms: Double,
    val failoverMs: Long?,
) {
    fun toCsv(): String = listOf(
        part, "\"$scenario\"", liveNodes, electionTimeoutMs, fsync, threads, totalOps,
        "%.1f".format(throughputOpsSec), "%.3f".format(avgMs), "%.3f".format(p50Ms),
        "%.3f".format(p95Ms), "%.3f".format(p99Ms), failoverMs?.toString() ?: "",
    ).joinToString(",")

    companion object {
        const val HEADER =
            "part,scenario,liveNodes,electionTimeoutMs,fsync,threads,totalOps," +
                "throughputOpsSec,avgMs,p50Ms,p95Ms,p99Ms,failoverMs"
    }
}

class BenchRunner(
    private val config: ClusterConfig,
    private val client: ClusterClient,
    private val bench: Bench,
    private val processes: NodeProcessManager,
) {
    fun runSuite(puts: Int, threads: Int) {
        println("=== benchmark suite: puts=$puts threads=$threads (this takes a while) ===")
        val rows = ArrayList<BenchRow>()
        val priorVerbose = config.verbose
        config.verbose = false

        config.fsync = true; config.electionMinMs = 150; config.electionMaxMs = 300
        freshCluster()
        warmup()
        rows += putRow("A", "5 nodes alive", liveNodes = 5, threads, puts)

        var leader = client.findLeader(5000)!!
        val followers = config.nodes.keys.filter { it != leader }
        processes.killNode(followers[0]); Thread.sleep(1200)
        rows += putRow("A", "4 nodes - 1 killed", liveNodes = 4, threads, puts)

        processes.killNode(followers[1]); Thread.sleep(1200)
        rows += putRow("A", "3 nodes - 2 killed (quorum)", liveNodes = 3, threads, puts)

        processes.startNode(followers[0], silent = true); processes.startNode(followers[1], silent = true)
        processes.waitForLeader(5000)

        for ((lo, hi) in listOf(150 to 300, 500 to 1000)) {
            client.setElectionTimeout(lo, hi)
            config.electionMinMs = lo.toLong(); config.electionMaxMs = hi.toLong()
            Thread.sleep(800)
            leader = client.findLeader(5000)!!
            val ms = bench.measureFailover { processes.crashLeader(leader) }
            processes.startNode(leader, silent = true); processes.waitForLeader(8000)
            rows += BenchRow("B", "electionTimeout $lo-$hi", liveNodes = 5, "$lo-$hi",
                "on", threads, 0, 0.0, 0.0, 0.0, 0.0, 0.0, ms)
            println("  Part B [$lo-$hi]: failoverMs=$ms")
        }
        client.setElectionTimeout(150, 300)
        config.electionMinMs = 150; config.electionMaxMs = 300

        config.fsync = true; freshCluster(); warmup()
        rows += putRow("C", "fsync on", liveNodes = 5, threads, puts, fsyncLabel = "on")

        config.fsync = false; freshCluster(); warmup()
        rows += putRow("C", "fsync off", liveNodes = 5, threads, puts, fsyncLabel = "off")

        config.verbose = priorVerbose
        config.fsync = true; freshCluster()

        writeCsv(rows)
        println("\n=== suite complete: ${rows.size} runs written to benchmarks/results.csv ===")
        rows.forEach { println("  " + it.toCsv()) }
    }

    private fun putRow(
        part: String, scenario: String, liveNodes: Int, threads: Int, puts: Int,
        fsyncLabel: String = if (config.fsync) "on" else "off",
    ): BenchRow {
        println("  running [$part] $scenario ...")
        val r = bench.putBench(threads, puts, 0.0)
        println("    " + r.pretty())
        return BenchRow(
            part, scenario, liveNodes, "${config.electionMinMs}-${config.electionMaxMs}", fsyncLabel,
            threads, puts, r.throughputOpsSec, r.avgMs, r.p50Ms, r.p95Ms, r.p99Ms, null,
        )
    }

    private fun freshCluster() {
        processes.shutdownCluster()
        processes.wipeData()
        for (id in config.nodes.keys) { processes.startNode(id, silent = true); Thread.sleep(60) }
        processes.waitForLeader(8000)
    }

    private fun warmup() {
        repeat(30) { client.put("warm$it", "$it", 3000) }
    }

    private fun writeCsv(rows: List<BenchRow>) {
        val file = File("benchmarks/results.csv")
        file.parentFile.mkdirs()
        file.writeText(BenchRow.HEADER + "\n" + rows.joinToString("\n") { it.toCsv() } + "\n")
    }
}
