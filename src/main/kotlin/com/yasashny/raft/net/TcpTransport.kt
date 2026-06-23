package com.yasashny.raft.net

import com.yasashny.raft.model.Message
import java.io.BufferedOutputStream
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

class TcpTransport(
    private val selfId: String,
    private val peers: Map<String, Address>,
    private val logger: (String) -> Unit = {},
) : Transport {
    @Volatile private var blockedPeers: Set<String> = emptySet()
    private val delayMs = AtomicInteger(0)
    @Volatile private var running = true

    private val links: Map<String, PeerLink> = peers.mapValues { (id, addr) -> PeerLink(id, addr) }

    init {
        links.values.forEach { it.start() }
    }

    override fun send(peerId: String, message: Message) {
        if (!running) return
        if (peerId in blockedPeers) return
        val link = links[peerId] ?: return
        link.enqueue(message.encode())
    }

    override fun setBlockedPeers(blocked: Set<String>) { blockedPeers = blocked }

    override fun setDelayMs(ms: Int) { delayMs.set(ms) }

    override fun close() {
        running = false
        links.values.forEach { it.stop() }
    }

    private inner class PeerLink(private val peerId: String, private val addr: Address) {
        private val queue = LinkedBlockingQueue<String>(QUEUE_CAPACITY)
        private val thread = Thread({ run() }, "tx-$selfId->$peerId").apply { isDaemon = true }
        private var socket: Socket? = null
        private var out: OutputStream? = null
        private var lastConnectFailNanos = 0L

        fun start() = thread.start()

        fun enqueue(line: String) {
            if (!queue.offer(line)) {
                queue.poll()
                queue.offer(line)
            }
        }

        fun stop() {
            thread.interrupt()
            closeSocket()
        }

        private fun run() {
            while (running) {
                val line = try {
                    queue.take()
                } catch (_: InterruptedException) {
                    break
                }
                if (peerId in blockedPeers) continue

                val d = delayMs.get()
                if (d > 0) {
                    try {
                        Thread.sleep(d.toLong())
                    } catch (_: InterruptedException) {
                        break
                    }
                    if (peerId in blockedPeers) continue
                }

                if (!ensureConnected()) continue
                try {
                    val o = out!!
                    o.write(line.toByteArray(StandardCharsets.UTF_8))
                    o.write('\n'.code)
                    o.flush()
                } catch (_: Exception) {
                    closeSocket()
                }
            }
            closeSocket()
        }

        private fun ensureConnected(): Boolean {
            if (out != null) return true

            if (System.nanoTime() - lastConnectFailNanos < RECONNECT_BACKOFF_NANOS) return false
            return try {
                val s = Socket()
                s.tcpNoDelay = true
                s.connect(InetSocketAddress(addr.host, addr.port), CONNECT_TIMEOUT_MS)
                socket = s
                out = BufferedOutputStream(s.getOutputStream())
                true
            } catch (_: Exception) {
                lastConnectFailNanos = System.nanoTime()
                closeSocket()
                false
            }
        }

        private fun closeSocket() {
            runCatching { out?.close() }
            runCatching { socket?.close() }
            out = null
            socket = null
        }
    }

    private companion object {
        const val QUEUE_CAPACITY = 8192
        const val CONNECT_TIMEOUT_MS = 300
        val RECONNECT_BACKOFF_NANOS = 200_000_000L
    }
}
