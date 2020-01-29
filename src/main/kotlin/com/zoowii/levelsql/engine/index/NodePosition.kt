package com.zoowii.levelsql.engine.index

import com.zoowii.levelsql.engine.store.Int32FromBytes
import com.zoowii.levelsql.engine.store.StoreSerializable
import com.zoowii.levelsql.engine.store.toBytes

data class NodePosition(var offset: Int) : StoreSerializable<NodePosition> {
    override fun fromBytes(bytes: ByteArray): Pair<NodePosition, ByteArray> {
        val (intVal, remaining) = Int32FromBytes(bytes)
        this.offset = intVal
        return Pair(this, remaining)
    }

    override fun toBytes(): ByteArray {
        return offset.toBytes()
    }

}
