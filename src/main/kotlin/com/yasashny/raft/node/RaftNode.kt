package com.yasashny.raft.node

import com.yasashny.raft.election.ElectionManager
import com.yasashny.raft.model.AppendEntries
import com.yasashny.raft.model.AppendEntriesResp
import com.yasashny.raft.model.ClientGet
import com.yasashny.raft.model.ClientPut
import com.yasashny.raft.model.ClientResponse
import com.yasashny.raft.model.ErrorCode
import com.yasashny.raft.model.Heal
import com.yasashny.raft.model.Kill
import com.yasashny.raft.model.LeaderInfo
import com.yasashny.raft.model.LeaderQuery
import com.yasashny.raft.model.LogDumpQuery
import com.yasashny.raft.model.LogDumpResp
import com.yasashny.raft.model.LogEntry
import com.yasashny.raft.model.Message
import com.yasashny.raft.model.PeerMessage
import com.yasashny.raft.model.RequestVote
import com.yasashny.raft.model.RequestVoteResp
import com.yasashny.raft.model.Role
import com.yasashny.raft.model.SetDelay
import com.yasashny.raft.model.SetElectionTimeout
import com.yasashny.raft.model.SetPartition
import com.yasashny.raft.model.StatusInfo
import com.yasashny.raft.model.StatusQuery
import com.yasashny.raft.net.ReplySink
import com.yasashny.raft.net.Transport
import com.yasashny.raft.replication.ReplicationManager
import com.yasashny.raft.storage.DurableState
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import kotlin.system.exitProcess

class RaftNode(
    val config: NodeConfig,
    private val durable: DurableState,
    private val transport: Transport,
    private val clock: () -> Long = System::nanoTime,
    private val verbose: Boolean = true,
    private val logSink: (String) -> Unit = { println(it) },
) {
    internal var currentTerm = 0
    internal var votedFor: String? = null
    internal val log = ArrayList<LogEntry>()

    internal var role = Role.FOLLOWER
    internal var commitIndex = 0
    internal var lastApplied = 0
    internal var currentLeader: String? = null

    internal val sentLength = HashMap<String, Int>()
    internal val ackedLength = HashMap<String, Int>()

    internal val votesGranted = HashSet<String>()

    private val stateMachine = KvStateMachine()

    private val pendingPuts = HashMap<Int, PendingPut>()

    internal var electionDeadline = 0L
    private var heartbeatDeadline = 0L

    private var lastHbTxLogNanos = 0L
    private var lastHbRxLogNanos = 0L

    private val queue = LinkedBlockingQueue<() -> Unit>()
    @Volatile private var running = true
    private var loopThread: Thread? = null

    internal var blockedPeers: Set<String> = emptySet()
    @Volatile private var linkDelayMs = 0

    private val election = ElectionManager(this)
    private val replication = ReplicationManager(this)

    private data class PendingPut(val requestId: String, val reply: ReplySink)

    fun start() {
        loadState()

        electionDeadline = clock() + (config.electionMaxMs * 2) * MS_TO_NS
        loopThread = Thread({ runLoop() }, "raft-loop-${config.id}").also { it.start() }
    }

    internal fun loadState() {
        val ps = durable.load()
        currentTerm = ps.currentTerm
        votedFor = ps.votedFor
        log.clear(); log.addAll(ps.log)
        role = Role.FOLLOWER
        commitIndex = 0
        lastApplied = 0
        stateMachine.clear()
        logState("recovered from disk: votedFor=$votedFor, log.len=${log.size}")
    }

    internal fun deliver(message: Message, reply: ReplySink = ReplySink {}) = dispatch(message, reply)

    internal fun forceElectionTimeout() = onElectionTimeout()

    internal fun kvGet(key: String): String? = stateMachine.get(key)

    fun stop() {
        running = false
        loopThread?.interrupt()
        transport.close()
        durable.close()
    }

    fun onInbound(message: Message, reply: ReplySink) {
        // Injected link delay is symmetric: it slows this node's peer RPCs in BOTH
        // directions. Outbound is delayed in the transport; inbound is delayed here, on
        // the (per-connection) RPC-server thread — never the raft loop. Without this, a
        // "delayed" leader still *hears* a higher term the instant a new leader emerges
        // and steps down immediately, so no kill window ever opens (see setDelay,
        // scenario 4). Only peer RPCs are delayed; client/admin requests stay responsive.
        val delay = linkDelayMs
        if (delay > 0 && message is PeerMessage) {
            try { Thread.sleep(delay.toLong()) } catch (_: InterruptedException) { return }
        }
        submit { dispatch(message, reply) }
    }

    internal fun submit(task: () -> Unit) {
        if (running) queue.offer(task)
    }

    private fun runLoop() {
        while (running) {
            val now = clock()
            val waitNanos = if (role == Role.LEADER) heartbeatDeadline - now else electionDeadline - now
            if (waitNanos <= 0) {
                runCatching { if (role == Role.LEADER) onHeartbeatTick() else onElectionTimeout() }
                    .onFailure { logState("loop timer error: ${it.message}") }
                continue
            }
            val task = try {
                queue.poll(waitNanos, TimeUnit.NANOSECONDS)
            } catch (_: InterruptedException) {
                if (!running) break else continue
            }
            if (task != null) {
                runCatching { task() }.onFailure { logState("loop task error: ${it.message}") }
            }
        }
    }

    private fun onElectionTimeout() = election.startElection()

    private fun onHeartbeatTick() {
        replication.replicateToAll()
        val now = clock()
        if (now - lastHbTxLogNanos >= HB_LOG_INTERVAL_NS) {
            lastHbTxLogNanos = now
            logState("♥ heartbeat → ${config.peers.keys} (commit=$commitIndex)")
        }
        heartbeatDeadline = now + config.heartbeatMs * MS_TO_NS
    }

    internal fun logHeartbeatReceived(leaderId: String, leaderCommit: Int) {
        val now = clock()
        if (now - lastHbRxLogNanos >= HB_LOG_INTERVAL_NS) {
            lastHbRxLogNanos = now
            logState("♥ from $leaderId (leaderCommit=$leaderCommit)")
        }
    }

    internal fun onElectedLeader() {
        replication.replicateToAll()
        heartbeatDeadline = clock() + config.heartbeatMs * MS_TO_NS
    }

    internal fun deferHeartbeat() {
        heartbeatDeadline = clock() + config.heartbeatMs * MS_TO_NS
    }

    private fun dispatch(message: Message, reply: ReplySink) {
        if (message is PeerMessage && message.source in blockedPeers) return
        when (message) {
            is RequestVote -> election.onRequestVote(message)
            is RequestVoteResp -> election.onRequestVoteResponse(message)
            is AppendEntries -> replication.onAppendEntries(message)
            is AppendEntriesResp -> replication.onAppendEntriesResponse(message)
            is ClientPut -> replication.onClientPut(message, reply)
            is ClientGet -> handleClientGet(message, reply)
            is LeaderQuery -> reply.send(
                LeaderInfo(message.requestId, config.id, role == Role.LEADER,
                    if (role == Role.LEADER) config.id else currentLeader, currentTerm)
            )
            is StatusQuery -> reply.send(
                StatusInfo(message.requestId, config.id, role.name, currentTerm, commitIndex, log.size)
            )
            is LogDumpQuery -> reply.send(LogDumpResp(message.requestId, config.id, ArrayList(log), commitIndex))
            is SetPartition -> { setBlockedPeers(message.blocked.toSet()); reply.send(ok(message.requestId)) }
            is Heal -> { setBlockedPeers(emptySet()); reply.send(ok(message.requestId)) }
            is SetDelay -> { setLinkDelay(message.delayMs); reply.send(ok(message.requestId)) }
            is SetElectionTimeout -> {
                config.electionMinMs = message.minMs.toLong()
                config.electionMaxMs = message.maxMs.toLong()
                resetElectionTimeout()
                logState("election timeout set to ${message.minMs}-${message.maxMs}ms")
                reply.send(ok(message.requestId))
            }
            is Kill -> handleKill(message, reply)
            else -> {}
        }
    }

    private fun handleClientGet(message: ClientGet, reply: ReplySink) {
        if (role == Role.LEADER) {
            reply.send(ClientResponse.ok(message.requestId, value = stateMachine.get(message.key)))
        } else {
            reply.send(ClientResponse.error(message.requestId, ErrorCode.NOT_LEADER, leaderHint = currentLeader))
        }
    }

    private fun handleKill(message: Kill, reply: ReplySink) {
        reply.send(ok(message.requestId))
        logState("received KILL → exiting (persistent state stays on disk)")

        Thread {
            Thread.sleep(50)
            runCatching { durable.close() }
            exitProcess(0)
        }.apply { isDaemon = true }.start()
    }

    private fun ok(requestId: String) = ClientResponse.ok(requestId)

    private fun setBlockedPeers(blocked: Set<String>) {
        blockedPeers = blocked
        transport.setBlockedPeers(blocked)
        logState(if (blocked.isEmpty()) "partition healed" else "partition: blocking $blocked")
    }

    private fun setLinkDelay(ms: Int) {
        linkDelayMs = ms
        transport.setDelayMs(ms)
        logState(if (ms > 0) "link delay set to ${ms}ms (inbound+outbound peer RPCs)" else "link delay cleared")
    }

    internal fun lastLogIndex(): Int = log.size
    internal fun lastLogTerm(): Int = if (log.isEmpty()) 0 else log.last().term
    internal fun majority(): Int = config.majority

    internal fun send(peerId: String, message: Message) {
        if (peerId != config.id) transport.send(peerId, message)
    }

    internal fun broadcast(message: Message) {
        for (peer in config.peers.keys) transport.send(peer, message)
    }

    internal fun persistMeta() = durable.saveMeta(currentTerm, votedFor)

    internal fun appendToLog(entries: List<LogEntry>) {
        log.addAll(entries)
        durable.appendLog(entries)
    }

    internal fun truncateLogTo(length: Int) {
        while (log.size > length) log.removeAt(log.size - 1)
        durable.truncateLog(length)
    }

    internal fun registerPendingPut(index: Int, requestId: String, reply: ReplySink) {
        pendingPuts[index] = PendingPut(requestId, reply)
    }

    internal fun advanceCommitIndex(newCommit: Int) {
        if (newCommit <= commitIndex) return
        for (i in commitIndex until newCommit) stateMachine.apply(log[i].command)
        commitIndex = newCommit
        lastApplied = newCommit
        val it = pendingPuts.entries.iterator()
        while (it.hasNext()) {
            val (idx, p) = it.next()
            if (idx <= commitIndex) {
                p.reply.send(ClientResponse.ok(p.requestId, committedIndex = commitIndex))
                it.remove()
            }
        }
    }

    internal fun stepDownIfHigher(term: Int): Boolean {
        if (term <= currentTerm) return false
        val wasActive = role != Role.FOLLOWER
        val wasLeader = role == Role.LEADER
        currentTerm = term
        votedFor = null
        persistMeta()
        currentLeader = null
        if (wasLeader) failPendingPuts()
        role = Role.FOLLOWER
        sentLength.clear(); ackedLength.clear(); votesGranted.clear()
        if (wasActive) resetElectionTimeout()
        logState("steps down: observed higher term $term")
        return true
    }

    private fun failPendingPuts() {
        for ((_, p) in pendingPuts) {
            p.reply.send(ClientResponse.error(p.requestId, ErrorCode.NOT_LEADER, leaderHint = currentLeader))
        }
        pendingPuts.clear()
    }

    internal fun resetElectionTimeout() {
        val min = config.electionMinMs
        val span = (config.electionMaxMs - min).coerceAtLeast(0)
        val randMs = if (span == 0L) min else min + ThreadLocalRandom.current().nextLong(span + 1)
        electionDeadline = clock() + randMs * MS_TO_NS
    }

    internal fun logState(msg: String) {
        val ts = java.time.LocalTime.now().withNano(0)
        logSink("$ts [${config.id}] term=$currentTerm $role commit=$commitIndex logLen=${log.size} | $msg")
    }

    internal fun logTrace(msg: String) {
        if (verbose) logState(msg)
    }

    private companion object {
        const val MS_TO_NS = 1_000_000L
        const val HB_LOG_INTERVAL_NS = 1_000_000_000L
    }
}
