package com.zoowii.levelsql.protocol.mysql

import java.util.*

// https://dev.mysql.com/doc/internals/en/describing-packets.html#type-lenenc_str
class ProtoStreamWriter @JvmOverloads constructor(initialCapacity: Int = DEFAULT_CAPACITY) {
    private var bytes: ByteArray
    private var offset = 0
    fun build(): ByteArray {
        return Arrays.copyOf(bytes, offset)
    }

    fun ensureCapacity(minimumCapacity: Int) {
        if (minimumCapacity <= 0) {
            throw IllegalArgumentException()
        }
        if (minimumCapacity - bytes.size <= 0) {
            return
        }
        var newCapacity = bytes.size * 2
        if (newCapacity - minimumCapacity < 0) newCapacity = minimumCapacity
        if (newCapacity < 0) {
            if (minimumCapacity < 0) { // overflow
                throw OutOfMemoryError()
            }
            newCapacity = Int.MAX_VALUE
        }
        bytes = Arrays.copyOf(bytes, newCapacity)
    }

    fun ensureSpace(len: Int) {
        ensureCapacity(offset + len)
    }

    fun length(): Int {
        return offset
    }

    fun putFixedLengthString(s: String, len: Int) {
        val source = s.toByteArray()
        if (source.size != len) {
            throw IllegalArgumentException()
        }
        ensureSpace(len)
        System.arraycopy(source, 0, bytes, offset, len)
        offset += len
    }

    fun putInt1(i: Byte): ProtoStreamWriter {
        putIntN(i.toLong(), 1)
        return this
    }

    fun putInt2(i: Short): ProtoStreamWriter {
        putIntN(i.toLong(), 2)
        return this
    }

    fun putInt3(i: Int): ProtoStreamWriter {
        putIntN(i.toLong(), 3)
        return this
    }

    fun putInt4(i: Int): ProtoStreamWriter {
        putIntN(i.toLong(), 4)
        return this
    }

    fun putInt6(i: Long): ProtoStreamWriter {
        putIntN(1, 6)
        return this
    }

    fun putInt8(i: Long): ProtoStreamWriter {
        putIntN(i, 8)
        return this
    }

    private fun putIntN(value: Long, size: Int) {
        var value = value
        var size = size
        if (size <= 0 || size > 8) {
            throw IllegalArgumentException()
        }
        ensureSpace(size)
        while (size-- > 0) {
            bytes[offset++] = (value and 0xFF).toByte()
            value = value shr 8
        }
    }

    fun putLengthEncodedInt(value: Long): ProtoStreamWriter {
        if (value < 251) {
            putInt1(value.toByte())
        } else if (value < 0x10000L) {
            putInt1(0xFC.toByte())
            putInt2(value.toShort())
        } else if (value < 0x1000000L) {
            putInt1(0xFD.toByte())
            putInt3(value.toInt())
        } else {
            putInt1(0xFE.toByte())
            putInt8(value)
        }
        return this
    }

    fun putLengthEncodedString(s: String): ProtoStreamWriter {
        val source = s.toByteArray()
        ensureSpace(1 + source.size)
        putLengthEncodedInt(source.size.toLong())
        System.arraycopy(source, 0, bytes, offset, source.size)
        offset += source.size
        return this
    }

    fun putNullTerminatedString(s: String): ProtoStreamWriter {
        val source = s.toByteArray()
        ensureSpace(source.size + 1)
        System.arraycopy(source, 0, bytes, offset, source.size)
        offset += source.size
        bytes[offset++] = 0x00
        return this
    }

    fun skip(len: Int): ProtoStreamWriter {
        ensureSpace(len)
        offset += len
        return this
    }

    fun putBytes(data: ByteArray): ProtoStreamWriter {
        System.arraycopy(data, 0, bytes, offset, data.size)
        offset += data.size
        return this
    }

    fun writeWithLength(src: ByteArray) {
        val length: Int = src.size
        if (length < 251) {
            putInt1(length.toByte())
        } else if (length < 0x10000L) {
            putInt1(0xFC.toByte())
            putInt2(length.toShort())
        } else if (length < 0x1000000L) {
            putInt1(0xFD.toByte())
            putInt3(length)
        } else {
            putInt1(0xFE.toByte())
            putInt8(length.toLong())
        }
        putBytes(src)
    }

    fun writeWithLength(src: ByteArray?,
                        nullValue: Byte) {
        if (src == null) {
            putInt1(nullValue)
        } else {
            writeWithLength(src)
        }
    }

    companion object {
        private const val DEFAULT_CAPACITY = 256
    }

    init {
        bytes = ByteArray(initialCapacity)
    }
}
