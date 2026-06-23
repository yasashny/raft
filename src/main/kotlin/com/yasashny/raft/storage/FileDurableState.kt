package com.yasashny.raft.storage

import com.yasashny.raft.json.Json
import com.yasashny.raft.model.LogEntry
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

class FileDurableState(
    dataDirRoot: String,
    nodeId: String,
    private val fsync: Boolean,
) : DurableState {
    private val dir: Path = Path.of(dataDirRoot, nodeId)
    private val metaFile: Path = dir.resolve("meta")
    private val metaTmp: Path = dir.resolve("meta.tmp")
    private val logFile: Path = dir.resolve("log")

    private val offsets = ArrayList<Long>()
    private lateinit var logRaf: RandomAccessFile
    private lateinit var logChannel: FileChannel

    override fun load(): PersistedState {
        Files.createDirectories(dir)

        var currentTerm = 0
        var votedFor: String? = null
        if (Files.exists(metaFile)) {
            val text = Files.readString(metaFile).trim()
            if (text.isNotEmpty()) {
                @Suppress("UNCHECKED_CAST")
                val m = Json.parse(text) as Map<String, Any?>
                currentTerm = (m["currentTerm"] as Number).toInt()
                votedFor = m["votedFor"] as String?
            }
        }

        val entries = ArrayList<LogEntry>()
        offsets.clear()
        offsets.add(0L)
        var consumed = 0L
        if (Files.exists(logFile)) {
            val bytes = Files.readAllBytes(logFile)
            var lineStart = 0
            var k = 0
            while (k < bytes.size) {
                if (bytes[k] == '\n'.code.toByte()) {
                    val line = String(bytes, lineStart, k - lineStart, StandardCharsets.UTF_8).trim()
                    if (line.isEmpty()) { lineStart = k + 1; k++; continue }
                    val entry = try {
                        @Suppress("UNCHECKED_CAST")
                        LogEntry.fromMap(Json.parse(line) as Map<String, Any?>)
                    } catch (_: Exception) {
                        break
                    }
                    entries.add(entry)
                    consumed = (k + 1).toLong()
                    offsets.add(consumed)
                    lineStart = k + 1
                }
                k++
            }
        }

        logRaf = RandomAccessFile(logFile.toFile(), "rw")
        logChannel = logRaf.channel
        if (logChannel.size() != consumed) {
            logChannel.truncate(consumed)
            if (fsync) logChannel.force(true)
        }
        logChannel.position(consumed)

        return PersistedState(currentTerm, votedFor, entries)
    }

    override fun saveMeta(currentTerm: Int, votedFor: String?) {
        val json = Json.encode(linkedMapOf<String, Any?>("currentTerm" to currentTerm, "votedFor" to votedFor))
        RandomAccessFile(metaTmp.toFile(), "rw").use { raf ->
            raf.setLength(0)
            raf.write(json.toByteArray(StandardCharsets.UTF_8))
            if (fsync) raf.channel.force(true)
        }
        Files.move(metaTmp, metaFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    }

    override fun appendLog(entries: List<LogEntry>) {
        if (entries.isEmpty()) return
        var pos = offsets.last()
        logChannel.position(pos)
        for (e in entries) {
            val lineBytes = (Json.encode(e.toMap()) + "\n").toByteArray(StandardCharsets.UTF_8)
            logChannel.write(java.nio.ByteBuffer.wrap(lineBytes))
            pos += lineBytes.size
            offsets.add(pos)
        }
        if (fsync) logChannel.force(true)
    }

    override fun truncateLog(newLength: Int) {
        require(newLength in 0 until offsets.size) { "truncate length $newLength out of range" }
        val newSize = offsets[newLength]
        logChannel.truncate(newSize)
        logChannel.position(newSize)
        while (offsets.size > newLength + 1) offsets.removeAt(offsets.size - 1)
        if (fsync) logChannel.force(true)
    }

    override fun close() {
        if (::logChannel.isInitialized) runCatching { logChannel.close() }
        if (::logRaf.isInitialized) runCatching { logRaf.close() }
    }
}
