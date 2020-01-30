package com.zoowii.levelsql.engine.store

import com.google.common.primitives.Longs
import com.zoowii.levelsql.engine.utils.ByteArrayStream

// 每张表有一个序列的row ids，从小到大自增。固定字节树。序列化成bytes后也保持整数的顺序。所以用大端序来序列化
class RowId(val data: ByteArray) {
    companion object {
        fun fromLong(value: Long): RowId {
            // big-endian representation
            return RowId(Longs.toByteArray(value))
        }
        fun fromBytes(stream: ByteArrayStream): RowId {
            val rowId = RowId(stream.remaining.copyOfRange(0, 8))
            stream.remaining = stream.remaining.copyOfRange(8, stream.remaining.size)
            return rowId
        }
    }

    fun toBytes(): ByteArray {
        return data
    }

    fun longValue(): Long {
        // big-endian representation
        return Longs.fromByteArray(data)
    }

    override fun toString(): String {
        return longValue().toString()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RowId

        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        return data.contentHashCode()
    }


}