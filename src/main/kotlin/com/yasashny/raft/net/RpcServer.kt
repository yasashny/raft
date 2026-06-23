package com.yasashny.raft.net

import com.yasashny.raft.model.Message
import com.yasashny.raft.model.decodeMessage
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.nio.charset.StandardCharsets

class ClientConnection(private val socket: Socket) : ReplySink {
    private val out: OutputStream = socket.getOutputStream()

    @Synchronized
    override fun send(message: Message) {
        try {
            out.write((message.encode() + "\n").toByteArray(StandardCharsets.UTF_8))
            out.flush()
        } catch (_: Exception) {
            runCatching { socket.close() }
        }
    }

    fun close() = runCatching { socket.close() }
}

class RpcServer(
    private val port: Int,
    private val name: String,
    private val logger: (String) -> Unit = {},
    private val handler: (Message, ClientConnection) -> Unit,
) {
    @Volatile private var running = true
    private lateinit var serverSocket: ServerSocket
    private lateinit var acceptor: Thread

    fun start() {
        serverSocket = ServerSocket().apply {
            reuseAddress = true
            bind(java.net.InetSocketAddress(port))
        }
        acceptor = Thread({ acceptLoop() }, "rpc-accept-$name").apply { isDaemon = true }
        acceptor.start()
    }

    private fun acceptLoop() {
        while (running) {
            val socket = try {
                serverSocket.accept()
            } catch (_: Exception) {
                if (running) continue else break
            }
            socket.tcpNoDelay = true
            Thread({ readLoop(socket) }, "rpc-conn-$name").apply { isDaemon = true }.start()
        }
    }

    private fun readLoop(socket: Socket) {
        val conn = ClientConnection(socket)
        try {
            val reader = BufferedReader(InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
            while (running) {
                val line = reader.readLine() ?: break
                if (line.isBlank()) continue
                val message = try {
                    decodeMessage(line)
                } catch (e: Exception) {
                    logger("bad message dropped: ${e.message}")
                    continue
                }
                handler(message, conn)
            }
        } catch (_: Exception) {
        } finally {
            conn.close()
        }
    }

    fun close() {
        running = false
        if (::serverSocket.isInitialized) runCatching { serverSocket.close() }
    }
}
