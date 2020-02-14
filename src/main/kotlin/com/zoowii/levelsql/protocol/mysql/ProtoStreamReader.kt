package com.zoowii.levelsql.protocol.mysql

import kotlin.experimental.and

// https://dev.mysql.com/doc/internals/en/describing-packets.html#type-lenenc_str
class ProtoStreamReader(private val buff: ByteArray) {
    private val NULL_LENGTH: Long = -1

    private var offset = 0
    fun getFixedLengthString(len: Int): String {
        val sb = StringBuilder(len)
        val end = offset + len
        while (offset < end) {
            sb.append(buff[offset++].toChar())
        }
        return sb.toString()
    }

    fun getInt1(): Byte = getIntN(1).toByte()

    fun getInt2(): Short = getIntN(2).toShort()

    fun getInt3(): Int = getIntN(3).toInt()

    fun getInt4(): Int = getIntN(4).toInt()

    fun getInt6(): Long = getIntN(6)

    fun getInt8(): Long = getIntN(8)

    private fun getIntN(len: Int): Long {
        var value: Long = 0
        for (i in offset + len - 1 downTo offset + 1) {
            value = value or buff[i].toLong() and 0xFF
            value = value shl 8
        }
        value = value or buff[offset].toLong() and 0xFF
        offset += len
        return value
    }

    //@formatter:off
    //@formatter:on
    fun getLengthEncodedInt(): Long {
            val head: Byte = getInt1() and 0xFF.toByte()
            //@formatter:off
            return if (head < 251) {
                head.toLong()
            } else if (head == 0xFC.toByte()) {
                getInt2().toLong()
            } else if (head == 0xFD.toByte()) {
                getInt3().toLong()
            } else {
                getInt8()
            }
            //@formatter:on
        }

    fun getLengthEncodedString(): String = getFixedLengthString(getLengthEncodedInt().toInt())

    fun getNullTerminatedString(): String {
            val sb = StringBuilder()
            while (buff[offset] != 0x00.toByte()) {
                sb.append(buff[offset++].toChar())
            }
            offset++
            return sb.toString()
        }

    fun getStringEOF(): String {
            val sb = StringBuilder()
            while (offset < buff.size) {
                sb.append(buff[offset++].toChar())
            }
            return sb.toString()
        }

    fun hasMore(): Boolean {
        return offset < buff.size
    }

    fun skip(len: Int) {
        offset += len
    }

    fun readBytes(): ByteArray {
        if (offset >= buff.size) {
            return byteArrayOf()
        }
        val ab = ByteArray(buff.size - offset)
        System.arraycopy(buff, offset, ab, 0, ab.size)
        offset = buff.size
        return ab
    }

    fun readBytes(length: Int): ByteArray? {
        val ab = ByteArray(length)
        System.arraycopy(buff, offset, ab, 0, length)
        offset += length
        return ab
    }

    fun readBytesWithNull(): ByteArray {
        val b: ByteArray = buff
        if (offset >= buff.size) {
            return byteArrayOf()
        }
        var tmpOffset = -1
        val length = buff.size
        for (i in offset until length) {
            if (b[i] == 0.toByte()) {
                tmpOffset = i
                break
            }
        }
        return when (tmpOffset) {
            -1 -> {
                val ab1 = ByteArray(length - offset)
                System.arraycopy(b, offset, ab1, 0, ab1.size)
                offset = length
                ab1
            }
            0 -> {
                offset++
                byteArrayOf()
            }
            else -> {
                val ab2 = ByteArray(tmpOffset - offset)
                System.arraycopy(b, offset, ab2, 0, ab2.size)
                offset = tmpOffset + 1
                ab2
            }
        }
    }

    fun readLength(): Long {
        val length: Int = buff[offset++].toInt() and 0xff
        return when (length) {
            251 -> NULL_LENGTH
            252 -> getInt2().toLong()
            253 -> getInt3().toLong()
            254 -> getInt8().toLong()
            else -> length.toLong()
        }
    }

    fun readBytesWithLength(): ByteArray? {
        val length = readLength().toInt()
        if (length == NULL_LENGTH.toInt()) {
            return null
        }
        if (length <= 0) {
            return byteArrayOf()
        }
        val ab = ByteArray(length)
        System.arraycopy(buff, offset, ab, 0, ab.size)
        offset += length
        return ab
    }


}
