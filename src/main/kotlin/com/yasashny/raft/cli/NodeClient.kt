package com.yasashny.raft.cli

import com.yasashny.raft.model.Message
import com.yasashny.raft.model.decodeMessage
import com.yasashny.raft.net.Address
import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.charset.StandardCharsets

class NodeClient(private val address: Address, private val timeoutMs: Int = 3000) : AutoCloseable {
    private var socket: Socket? = null
    private var reader: BufferedReader? = null
    private var out: OutputStream? = null

    private fun ensureConnected() {
        val s = socket
        if (s != null && s.isConnected && !s.isClosed) return
        close()
        val fresh = Socket()
        fresh.tcpNoDelay = true
        fresh.connect(InetSocketAddress(address.host, address.port), timeoutMs)
        fresh.soTimeout = timeoutMs
        socket = fresh
        reader = BufferedReader(InputStreamReader(fresh.getInputStream(), StandardCharsets.UTF_8))
        out = BufferedOutputStream(fresh.getOutputStream())
    }

    fun request(message: Message): Message {
        ensureConnected()
        val o = out!!
        o.write((message.encode() + "\n").toByteArray(StandardCharsets.UTF_8))
        o.flush()
        val line = reader!!.readLine() ?: throw IOException("connection closed by ${address.host}:${address.port}")
        return decodeMessage(line)
    }

    override fun close() {
        runCatching { socket?.close() }
        socket = null; reader = null; out = null
    }
}
