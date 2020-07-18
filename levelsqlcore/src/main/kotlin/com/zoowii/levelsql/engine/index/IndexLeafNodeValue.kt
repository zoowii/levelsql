package com.zoowii.levelsql.engine.index

import com.zoowii.levelsql.engine.store.*
import com.zoowii.levelsql.engine.utils.ByteArrayStream
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
        fun fromBytes(stream: ByteArrayStream): IndexLeafNodeValue {
            val rowId = RowId.fromBytes(stream)
            val keyBytes = stream.unpackByteArray()
            val valueBytes = stream.unpackByteArray()
            return IndexLeafNodeValue(rowId, keyBytes, valueBytes)
        }
    }

    override fun fromBytes(stream: ByteArrayStream): IndexLeafNodeValue {
        return IndexLeafNodeValue.fromBytes(stream)
    }

    fun compareTo(other: IndexLeafNodeValue): Int {
        return compareNodeKey(this.toBytes(), other.toBytes())
    }

    override fun toString(): String {
        return "${rowId.toString()}-${bytesToHex(key)}-${bytesToHex(value)}"
    }
}