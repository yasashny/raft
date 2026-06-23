package com.yasashny.raft.cli

import com.yasashny.raft.model.ClientGet
import com.yasashny.raft.model.ClientPut
import com.yasashny.raft.model.ClientResponse
import com.yasashny.raft.model.ErrorCode
import com.yasashny.raft.net.Address
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

data class BenchResult(
    val completedOps: Int,
    val errorOps: Int,
    val throughputOpsSec: Double,
    val avgMs: Double,
    val p50Ms: Double,
    val p95Ms: Double,
    val p99Ms: Double,
) {
    fun pretty(): String =
        "ops=%d errors=%d  throughput=%.0f ops/s  avg=%.2fms  p50=%.2fms  p95=%.2fms  p99=%.2fms"
            .format(completedOps, errorOps, throughputOpsSec, avgMs, p50Ms, p95Ms, p99Ms)
}

class Bench(private val client: ClusterClient, private val nodes: Map<String, Address>) {
    fun putBench(threads: Int, totalOps: Int, readRatio: Double): BenchResult {
        val initialLeader = client.findLeader(5000) ?: error("no leader available for benchmark")
        val leaderRef = AtomicReference(initialLeader)
        val remaining = AtomicInteger(totalOps)
        val errors = AtomicInteger(0)
        val allLatencies = ArrayList<Long>(totalOps)

        val start = System.nanoTime()
        val workers = (0 until threads).map { tid ->
            Thread({ worker(tid, remaining, leaderRef, errors, allLatencies, readRatio) }, "bench-$tid")
                .apply { start() }
        }
        workers.forEach { it.join() }
        val wallSec = (System.nanoTime() - start) / 1e9

        return summarize(allLatencies, errors.get(), wallSec)
    }

    private fun worker(
        tid: Int,
        remaining: AtomicInteger,
        leaderRef: AtomicReference<String>,
        errors: AtomicInteger,
        sink: MutableList<Long>,
        readRatio: Double,
    ) {
        val local = ArrayList<Long>()
        var conn: NodeClient? = null
        val rnd = java.util.concurrent.ThreadLocalRandom.current()
        try {
            while (true) {
                val op = remaining.getAndDecrement()
                if (op <= 0) break
                val isRead = readRatio > 0 && rnd.nextDouble() < readRatio
                val key = "k${tid}_$op"
                val t0 = System.nanoTime()
                var done = false
                var attempts = 0
                while (!done && attempts < 100) {
                    attempts++
                    val leader = leaderRef.get()
                    try {
                        val c = conn ?: NodeClient(nodes.getValue(leader)).also { conn = it }
                        val reqId = UUID.randomUUID().toString()
                        val resp = c.request(
                            if (isRead) ClientGet(reqId, key) else ClientPut(reqId, key, "v$op")
                        ) as ClientResponse
                        when {
                            resp.status == "OK" -> done = true
                            resp.error == ErrorCode.NOT_LEADER -> {
                                c.close(); conn = null
                                val nl = resp.leaderHint ?: client.findLeader(1000)
                                if (nl != null) leaderRef.set(nl)
                                Thread.sleep(20)
                            }
                            else -> { errors.incrementAndGet(); done = true }
                        }
                    } catch (_: Exception) {
                        conn?.close(); conn = null
                        val nl = client.findLeader(1000)
                        if (nl != null) leaderRef.set(nl)
                        Thread.sleep(20)
                    }
                }
                if (!done) errors.incrementAndGet()
                local.add(System.nanoTime() - t0)
            }
        } finally {
            conn?.close()
            synchronized(sink) { sink.addAll(local) }
        }
    }

    fun measureFailover(overallTimeoutMs: Long = 20000, killAction: () -> Unit): Long {
        val t0 = System.nanoTime()
        killAction()
        val resp = client.put("__failover_probe__", "1", overallTimeoutMs)
        val elapsedMs = (System.nanoTime() - t0) / 1_000_000
        return if (resp.status == "OK") elapsedMs else -1
    }

    private fun summarize(latencies: List<Long>, errors: Int, wallSec: Double): BenchResult {
        if (latencies.isEmpty()) return BenchResult(0, errors, 0.0, 0.0, 0.0, 0.0, 0.0)
        val sortedMs = latencies.map { it / 1_000_000.0 }.sorted()
        val completed = latencies.size - errors
        fun pct(p: Double) = sortedMs[((p * (sortedMs.size - 1)).toInt()).coerceIn(0, sortedMs.size - 1)]
        return BenchResult(
            completedOps = completed.coerceAtLeast(0),
            errorOps = errors,
            throughputOpsSec = if (wallSec > 0) completed / wallSec else 0.0,
            avgMs = sortedMs.average(),
            p50Ms = pct(0.50),
            p95Ms = pct(0.95),
            p99Ms = pct(0.99),
        )
    }
}
