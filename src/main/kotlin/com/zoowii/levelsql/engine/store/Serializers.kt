package com.zoowii.levelsql.engine.store

import com.google.common.primitives.Ints
import com.zoowii.levelsql.engine.exceptions.SerializeException
import java.io.ByteArrayOutputStream
import java.util.*
import kotlin.experimental.and

interface StoreSerializable<T> {
    fun toBytes(): ByteArray
    // throws Exception
    fun fromBytes(bytes: ByteArray): Pair<T, ByteArray>
}

fun Int.toBytes(): ByteArray {
    // big-endian representation
    return Ints.toByteArray(this)
}

fun Int32FromBytes(data: ByteArray): Pair<Int, ByteArray> {
    // big-endian representation
    return Pair(Ints.fromByteArray(data), data.copyOfRange(4, data.size))
}

fun String.toBytes(): ByteArray {
    val out = ByteArrayOutputStream()
    out.write(length.toBytes())
    for(c in this.toCharArray()) {
        out.write(c.toInt())
    }
    return out.toByteArray()
}

fun StringFromBytes(data: ByteArray): Pair<String, ByteArray> {
    val (len, remaining) = Int32FromBytes(data)
    val chars = data.copyOfRange(4, len + 4)
    return Pair(String(chars), remaining.copyOfRange(len, remaining.size))
}

fun ByteArray.toBytes(): ByteArray {
    val out = ByteArrayOutputStream()
    out.write(size.toBytes())
    for(i in 0 until size) {
        val c = this[i]
        out.write(c.toInt())
    }
    return out.toByteArray()
}

fun ByteArrayFromBytes(data: ByteArray): Pair<ByteArray, ByteArray> {
    val (len, _) = Int32FromBytes(data.copyOfRange(0, 4))
    val chars = data.copyOfRange(4, len + 4)
    return Pair(chars, data.copyOfRange(len+4, data.size))
}

fun Boolean.toBytes(): ByteArray {
    return byteArrayOf(if(this) 1 else 0)
}

fun BooleanFromBytes(data: ByteArray): Pair<Boolean, ByteArray> {
    val c = data[0]
    val b = if(c>0) true else false
    return Pair(b, data.copyOfRange(1, data.size))
}

private val HEX_ARRAY = "0123456789ABCDEF".toCharArray()

fun bytesToHex(bytes: ByteArray): String {
    val hexChars = CharArray(bytes.size * 2)
    for (j in bytes.indices) {
        val v = (bytes[j].toInt() and 0xFF)
        hexChars[j * 2] = HEX_ARRAY[v.ushr(4)]
        hexChars[j * 2 + 1] = HEX_ARRAY[(v and 0x0F)]
    }
    return String(hexChars)
}