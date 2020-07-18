package com.zoowii.levelsql.oss

import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream

class RandomAccessStream(private val origin: InputStream) {
    private val buffer = ByteArrayOutputStream()
    private var bufferBytes = byteArrayOf()
    private var position: Long = 0

    @Throws(IOException::class)
    fun seek(pos: Long) {
        if (buffer.size() > pos) {
            position = pos
            return
        }
        var nextBytesCount = pos - buffer.size() + 1
        val available = origin.available()
        if(nextBytesCount<available) {
            nextBytesCount = available.toLong()
        }
        val nextBuf = origin.readNBytes(nextBytesCount.toInt())
        buffer.writeBytes(nextBuf)
        bufferBytes = buffer.toByteArray()
        position = pos
    }

    @Throws(IOException::class)
    fun close() {
        origin.close()
    }

    fun offset(): Long {
        return position
    }

    @Throws(IOException::class)
    fun eof(offset: Long): Boolean {
        if (offset < buffer.size()) {
            return false
        }
        try {
            val old = position
            seek(offset)
            position = old
            return offset >= buffer.size()
        } catch (e: IOException) {
            // FIXME
            return true
        }
    }

    @Throws(IOException::class)
    fun read(): Int {
        if (position >= buffer.size()) {
            if (eof(position + 1)) {
                return -1
            }
            val old = position
            seek(position + 1)
            position = old
        }
        val c = bufferBytes[position.toInt()].toInt()
        position += 1
        return c
    }

    @Throws(IOException::class)
    fun readLine(): String? {
        val input = StringBuilder()
        var c = -1
        var eol = false

        while (!eol) {
            when (read().also { c = it }) {
                -1, '\n'.toInt() -> eol = true
                '\r'.toInt() -> {
                    eol = true
                    val cur: Long = position
                    if (read() != '\n'.toInt()) {
                        seek(cur)
                    }
                }
                else -> input.append(c.toChar())
            }
        }

        return if (c == -1 && input.length == 0) {
            null
        } else input.toString()
    }
}