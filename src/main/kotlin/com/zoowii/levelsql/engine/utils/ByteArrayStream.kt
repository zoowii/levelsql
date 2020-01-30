package com.zoowii.levelsql.engine.utils

import com.zoowii.levelsql.engine.store.BooleanFromBytes
import com.zoowii.levelsql.engine.store.ByteArrayFromBytes
import com.zoowii.levelsql.engine.store.Int32FromBytes
import com.zoowii.levelsql.engine.store.StringFromBytes

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
    fun unpackByteArray(): ByteArray {
        val (value, other) = ByteArrayFromBytes(remaining)
        remaining = other
        return value
    }
}