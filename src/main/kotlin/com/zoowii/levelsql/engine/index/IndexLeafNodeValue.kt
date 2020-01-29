package com.zoowii.levelsql.engine.index

import com.zoowii.levelsql.engine.store.*
import com.zoowii.levelsql.engine.utils.compareNodeKey
import java.io.ByteArrayOutputStream

data class IndexLeafNodeValue(val rowId: RowId, val key: ByteArray, val value: ByteArray) : StoreSerializable<IndexLeafNodeValue> {
    override fun toBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(rowId.toBytes())
        out.write(key.toBytes())
        out.write(value.toBytes())
        return out.toByteArray()
    }

    companion object {
        fun fromBytes(bytes: ByteArray): Pair<IndexLeafNodeValue, ByteArray> {
            val (rowId, remaining1) = RowId.fromBytes(bytes)
            val (keyBytes, remaining2) = ByteArrayFromBytes(remaining1)
            val (valueBytes, remaining3) = ByteArrayFromBytes(remaining2)
            return Pair(IndexLeafNodeValue(rowId, keyBytes, valueBytes), remaining3)
        }
    }

    override fun fromBytes(bytes: ByteArray): Pair<IndexLeafNodeValue, ByteArray> {
        return IndexLeafNodeValue.fromBytes(bytes)
    }

    fun compareTo(other: IndexLeafNodeValue): Int {
        return compareNodeKey(this.toBytes(), other.toBytes())
    }

    override fun toString(): String {
        return "${rowId.toString()}-${bytesToHex(key)}-${bytesToHex(value)}"
    }
}