package com.yasashny.raft.cli

class Cli(private val config: ClusterConfig) {
    private val client = ClusterClient(config.nodes)
    private val bench = Bench(client, config.nodes)
    private val processes = NodeProcessManager(config, client)
    private val benchRunner = BenchRunner(config, client, bench, processes)

    fun repl() {
        printBanner()
        val reader = System.`in`.bufferedReader()
        while (true) {
            print("raft> "); System.out.flush()
            val line = reader.readLine() ?: break
            val trimmed = line.trim()
            if (trimmed.isEmpty()) continue
            if (trimmed.startsWith("#")) continue
            try {
                if (!handle(trimmed)) break
            } catch (e: Exception) {
                println("error: ${e.message}")
            }
        }
        println("bye")
    }

    private fun handle(line: String): Boolean {
        val p = line.split(Regex("\\s+"))
        when (p[0]) {
            "help" -> printHelp()
            "startCluster" -> startCluster(clean = p.contains("--clean"))
            "shutdownCluster" -> processes.shutdownCluster()
            "startNode" -> processes.startNode(p[1])
            "killNode" -> processes.killNode(p[1])
            "put" -> doPut(p[1], p.drop(2).joinToString(" "))
            "putTo" -> doPutTo(p[1], p[2], p.drop(3).joinToString(" "))
            "get" -> doGet(p[1])
            "leader" -> showLeaders()
            "status" -> showStatus()
            "logDump" -> showLogDump(flagValue(p, "--node") ?: config.nodes.keys.first())
            "partition" -> doPartition(p[1], p[2])
            "healPartition" -> { client.heal(); println("partition healed on all nodes") }
            "setDelay" -> { client.setDelay(p[1], p[2].toInt()); println("set link delay of ${p[1]} to ${p[2]}ms (both directions)") }
            "setElectionTimeout" -> doSetElectionTimeout(p[1].toInt(), p[2].toInt())
            "wipe" -> processes.wipeData()
            "bench" -> doBench(p)
            "benchFailover" -> doBenchFailover()
            "benchSuite" -> benchRunner.runSuite(
                puts = flagValue(p, "--puts")?.toInt() ?: 10000,
                threads = flagValue(p, "--threads")?.toInt() ?: 100,
            )
            "sleep" -> Thread.sleep(p[1].toLong())
            "exit", "quit" -> return false
            else -> println("unknown command: ${p[0]} (try 'help')")
        }
        return true
    }

    private fun startCluster(clean: Boolean) {
        if (clean) processes.wipeData()
        for (id in config.nodes.keys) {
            processes.startNode(id, silent = true)
            Thread.sleep(60)
        }
        println("started ${config.nodes.keys} (fsync=${config.fsync}, election=${config.electionMinMs}-${config.electionMaxMs}ms)")
        processes.waitForLeader(5000)
    }

    private fun doPut(key: String, value: String) {
        val resp = client.put(key, value)
        if (resp.status == "OK") println("OK (committed at index ${resp.committedIndex})")
        else println("ERROR ${resp.error}${resp.leaderHint?.let { " (leaderHint=$it)" } ?: ""}")
    }

    private fun doPutTo(nodeId: String, key: String, value: String) {
        val resp = client.putTo(nodeId, key, value)
        if (resp.status == "OK") println("OK (committed at index ${resp.committedIndex})")
        else println("ERROR ${resp.error}${resp.leaderHint?.let { " (leaderHint=$it)" } ?: ""}  [target=$nodeId]")
    }

    private fun doGet(key: String) {
        val resp = client.get(key)
        when {
            resp.status == "OK" && resp.value != null -> println(resp.value)
            resp.status == "OK" -> println("(nil)")
            else -> println("ERROR ${resp.error}${resp.leaderHint?.let { " (leaderHint=$it)" } ?: ""}")
        }
    }

    private fun showLeaders() {
        val infos = client.leaders()
        for (id in config.nodes.keys) {
            val info = infos.find { it.nodeId == id }
            if (info == null) println("  %-4s DOWN".format(id))
            else println("  %-4s %-9s term=%d  knownLeader=%s".format(
                id, if (info.isLeader) "LEADER" else "follower", info.term, info.leaderId ?: "?"))
        }
        val leader = infos.filter { it.isLeader }.maxByOrNull { it.term }
        println(if (leader != null) "=> leader is ${leader.nodeId} in term ${leader.term}" else "=> no leader")
    }

    private fun showStatus() {
        println("  %-4s %-9s %-5s %-11s %-9s".format("node", "role", "term", "commitIndex", "logLength"))
        for ((id, st) in client.status()) {
            if (st == null) println("  %-4s %-9s".format(id, "DOWN"))
            else println("  %-4s %-9s %-5d %-11d %-9d".format(id, st.role, st.term, st.commitIndex, st.logLength))
        }
    }

    private fun showLogDump(nodeId: String) {
        val dump = client.logDump(nodeId)
        if (dump == null) { println("$nodeId DOWN"); return }
        println("log of $nodeId (commitIndex=${dump.commitIndex}):")
        println("  %-6s %-5s %s".format("index", "term", "command"))
        dump.entries.forEachIndexed { i, e ->
            val mark = if (i + 1 <= dump.commitIndex) "*" else " "
            println("$mark %-6d %-5d %s".format(i + 1, e.term, e.command))
        }
        println("  (* = committed)")
    }

    private fun doPartition(g1Spec: String, g2Spec: String) {
        val g1 = g1Spec.split(",").filter { it.isNotBlank() }
        val g2 = g2Spec.split(",").filter { it.isNotBlank() }
        client.partition(g1, g2)
        println("partitioned $g1 | $g2")
    }

    private fun doSetElectionTimeout(min: Int, max: Int) {
        client.setElectionTimeout(min, max)
        config.electionMinMs = min.toLong()
        config.electionMaxMs = max.toLong()
        println("election timeout set to $min-${max}ms on all nodes")
    }

    private fun doBench(p: List<String>) {
        val puts = flagValue(p, "--puts")?.toInt() ?: 1000
        val threads = flagValue(p, "--threads")?.toInt() ?: 50
        val readRatio = flagValue(p, "--read-ratio")?.toDouble() ?: 0.0
        println("running bench: puts=$puts threads=$threads readRatio=$readRatio ...")
        val r = bench.putBench(threads, puts, readRatio)
        println(r.pretty())
    }

    private fun doBenchFailover() {
        val leader = client.findLeader(5000) ?: run { println("no leader"); return }
        println("crashing leader $leader and measuring failover ...")
        val ms = bench.measureFailover { processes.crashLeader(leader) }
        println("failoverMs = $ms")
        println("restarting $leader ...")
        processes.startNode(leader, silent = true)
        processes.waitForLeader(5000)
    }

    private fun flagValue(parts: List<String>, name: String): String? {
        val i = parts.indexOf(name)
        return if (i >= 0 && i + 1 < parts.size) parts[i + 1] else null
    }

    private fun printBanner() {
        println("Raft KV CLI — nodes ${config.nodes.keys}, dataDir=${config.dataDirRoot}")
        println("type 'help' for commands; 'startCluster --clean' to begin.")
    }

    private fun printHelp() = println(
        """
        Cluster:
          startCluster [--clean]      start all 5 nodes (optionally wipe data first)
          shutdownCluster             stop all nodes
          startNode <id>              (re)start one node (recovers state from disk)
          killNode <id>               System.exit a node (data on disk kept)
          wipe                        delete the data dir (cluster must be down)
        Client:
          put <key> <value>           write through the leader
          putTo <id> <key> <value>    write to a SPECIFIC node (no redirect; probe minority/divergence)
          get <key>                   read from the leader
          leader                      who is leader, in which term
          status                      role/term/commitIndex/logLength per node
          logDump [--node <id>]       dump a node's log (index, term, command)
        Fault injection:
          partition <g1> <g2>         block traffic between comma-separated groups (e.g. n1,n2 n3,n4,n5)
          healPartition               clear all partitions
          setDelay <id> <ms>          delay a node's peer RPCs both ways (open a kill window)
          setElectionTimeout <a> <b>  set the election-timeout window (ms) on all nodes
        Benchmarks:
          bench --puts N --threads T [--read-ratio r]   ad-hoc throughput run
          benchFailover                                 kill leader, measure failoverMs, restart
          benchSuite [--puts N] [--threads T]           full required suite → benchmarks/results.csv
        Other: sleep <ms>, help, exit
        """.trimIndent()
    )
}
