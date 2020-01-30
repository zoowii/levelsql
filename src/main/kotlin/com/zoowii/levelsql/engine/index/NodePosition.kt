package com.zoowii.levelsql.engine.index

import com.zoowii.levelsql.engine.store.Int32FromBytes
import com.zoowii.levelsql.engine.store.StoreSerializable
import com.zoowii.levelsql.engine.store.toBytes
import com.zoowii.levelsql.engine.utils.ByteArrayStream

data class NodePosition(var offset: Int) : StoreSerializable<NodePosition> {
    override fun fromBytes(stream: ByteArrayStream): NodePosition {
        this.offset = stream.unpackInt32()
        return this
    }

    override fun toBytes(): ByteArray {
        return offset.toBytes()
    }

}
