package com.yasashny.raft.json

object Json {
    fun encode(value: Any?): String {
        val sb = StringBuilder()
        encodeInto(value, sb)
        return sb.toString()
    }

    private fun encodeInto(value: Any?, sb: StringBuilder) {
        when (value) {
            null -> sb.append("null")
            is String -> encodeString(value, sb)
            is Boolean -> sb.append(value.toString())
            is Int, is Long, is Short, is Byte -> sb.append(value.toString())
            is Double, is Float -> sb.append(value.toString())
            is Map<*, *> -> {
                sb.append('{')
                var first = true
                for ((k, v) in value) {
                    if (!first) sb.append(',')
                    first = false
                    encodeString(k.toString(), sb)
                    sb.append(':')
                    encodeInto(v, sb)
                }
                sb.append('}')
            }
            is Iterable<*> -> {
                sb.append('[')
                var first = true
                for (v in value) {
                    if (!first) sb.append(',')
                    first = false
                    encodeInto(v, sb)
                }
                sb.append(']')
            }
            else -> throw IllegalArgumentException("Cannot encode value of type ${value::class}")
        }
    }

    private fun encodeString(s: String, sb: StringBuilder) {
        sb.append('"')
        for (c in s) {
            when (c) {
                '"' -> sb.append("\\\"")
                '\\' -> sb.append("\\\\")
                '\n' -> sb.append("\\n")
                '\r' -> sb.append("\\r")
                '\t' -> sb.append("\\t")
                else ->
                    if (c < ' ') sb.append("\\u").append(c.code.toString(16).padStart(4, '0'))
                    else sb.append(c)
            }
        }
        sb.append('"')
    }

    fun parse(text: String): Any? = Parser(text).parseValue()

    private class Parser(private val s: String) {
        private var i = 0

        fun parseValue(): Any? {
            skipWs()
            if (i >= s.length) throw parseError("unexpected end of input")
            return when (s[i]) {
                '{' -> parseObject()
                '[' -> parseArray()
                '"' -> parseString()
                't', 'f' -> parseBoolean()
                'n' -> parseNull()
                else -> parseNumber()
            }
        }

        private fun parseObject(): Map<String, Any?> {
            expect('{')
            val map = LinkedHashMap<String, Any?>()
            skipWs()
            if (peek() == '}') { i++; return map }
            while (true) {
                skipWs()
                val key = parseString()
                skipWs()
                expect(':')
                val value = parseValue()
                map[key] = value
                skipWs()
                when (val c = next()) {
                    ',' -> continue
                    '}' -> break
                    else -> throw parseError("expected ',' or '}' but found '$c'")
                }
            }
            return map
        }

        private fun parseArray(): List<Any?> {
            expect('[')
            val list = ArrayList<Any?>()
            skipWs()
            if (peek() == ']') { i++; return list }
            while (true) {
                list.add(parseValue())
                skipWs()
                when (val c = next()) {
                    ',' -> continue
                    ']' -> break
                    else -> throw parseError("expected ',' or ']' but found '$c'")
                }
            }
            return list
        }

        private fun parseString(): String {
            expect('"')
            val sb = StringBuilder()
            while (true) {
                if (i >= s.length) throw parseError("unterminated string")
                val c = s[i++]
                when (c) {
                    '"' -> return sb.toString()
                    '\\' -> {
                        val esc = s[i++]
                        when (esc) {
                            '"' -> sb.append('"')
                            '\\' -> sb.append('\\')
                            '/' -> sb.append('/')
                            'n' -> sb.append('\n')
                            'r' -> sb.append('\r')
                            't' -> sb.append('\t')
                            'b' -> sb.append('\b')
                            'f' -> sb.append('\u000C')
                            'u' -> {
                                val hex = s.substring(i, i + 4)
                                i += 4
                                sb.append(hex.toInt(16).toChar())
                            }
                            else -> throw parseError("invalid escape '\\$esc'")
                        }
                    }
                    else -> sb.append(c)
                }
            }
        }

        private fun parseNumber(): Any {
            val start = i
            if (peek() == '-') i++
            while (i < s.length && s[i].isDigit()) i++
            var isDecimal = false
            if (i < s.length && s[i] == '.') {
                isDecimal = true
                i++
                while (i < s.length && s[i].isDigit()) i++
            }
            if (i < s.length && (s[i] == 'e' || s[i] == 'E')) {
                isDecimal = true
                i++
                if (i < s.length && (s[i] == '+' || s[i] == '-')) i++
                while (i < s.length && s[i].isDigit()) i++
            }
            val token = s.substring(start, i)
            if (token.isEmpty() || token == "-") throw parseError("invalid number near position $start")
            return if (isDecimal) token.toDouble() else token.toLong()
        }

        private fun parseBoolean(): Boolean =
            if (s.startsWith("true", i)) { i += 4; true }
            else if (s.startsWith("false", i)) { i += 5; false }
            else throw parseError("invalid literal")

        private fun parseNull(): Any? {
            if (s.startsWith("null", i)) { i += 4; return null }
            throw parseError("invalid literal")
        }

        private fun skipWs() {
            while (i < s.length && s[i].isWhitespace()) i++
        }

        private fun peek(): Char = if (i < s.length) s[i] else ' '
        private fun next(): Char = if (i < s.length) s[i++] else throw parseError("unexpected end of input")
        private fun expect(c: Char) {
            val actual = next()
            if (actual != c) throw parseError("expected '$c' but found '$actual'")
        }

        private fun parseError(msg: String) = IllegalArgumentException("JSON parse error: $msg")
    }
}

fun Map<String, Any?>.str(key: String): String? = this[key] as String?
fun Map<String, Any?>.reqStr(key: String): String = str(key) ?: error("missing string field '$key'")
fun Map<String, Any?>.int(key: String): Int = (this[key] as Number).toInt()
fun Map<String, Any?>.intOr(key: String, default: Int): Int = (this[key] as Number?)?.toInt() ?: default
fun Map<String, Any?>.bool(key: String): Boolean = this[key] as Boolean

@Suppress("UNCHECKED_CAST")
fun Map<String, Any?>.list(key: String): List<Any?> = (this[key] as List<Any?>?) ?: emptyList()
