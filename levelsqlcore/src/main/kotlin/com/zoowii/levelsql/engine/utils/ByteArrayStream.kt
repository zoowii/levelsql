package com.zoowii.levelsql.engine.utils

import com.zoowii.levelsql.engine.store.*

class ByteArrayStream(private val initData: ByteArray) {
    var remaining = initData

    fun unpackString(): String {
        val (str, other) = StringFromBytes(remaining)
        remaining = other
        return str
    }
    fun unpackBoolean(): Boolean {
        val (b, other) = BooleanFromBytes(remaining)
        remaining = other
        return b
    }
    fun unpackInt32(): Int {
        val (value, other) = Int32FromBytes(remaining)
        remaining = other
        return value
    }
    fun unpackInt64(): Long {
        val (value, other) = Int64FromBytes(remaining)
        remaining = other
        return value
    }
    fun unpackByteArray(): ByteArray {
        val (value, other) = ByteArrayFromBytes(remaining)
        remaining = other
        return value
    }
}