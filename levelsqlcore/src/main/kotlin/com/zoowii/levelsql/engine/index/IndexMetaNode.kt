package com.zoowii.levelsql.engine.index

import com.zoowii.levelsql.engine.store.BooleanFromBytes
import com.zoowii.levelsql.engine.store.Int32FromBytes
import com.zoowii.levelsql.engine.store.StoreSerializable
import com.zoowii.levelsql.engine.store.toBytes
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import java.io.ByteArrayOutputStream


data class IndexMetaNode(var nodesCount: Int, var nodeBytesSize: Int, var nodeSubMaxCount: Int, var ascSort: Boolean, var rootNodePosition: NodePosition) : StoreSerializable<IndexMetaNode> {
    override fun toBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(nodesCount.toBytes())
        out.write(nodeBytesSize.toBytes())
        out.write(nodeSubMaxCount.toBytes())
        out.write(ascSort.toBytes())
        out.write(rootNodePosition.toBytes())
        return out.toByteArray()
    }

    override fun fromBytes(stream: ByteArrayStream): IndexMetaNode {
        this.nodesCount = stream.unpackInt32()
        this.nodeBytesSize = stream.unpackInt32()
        this.nodeSubMaxCount = stream.unpackInt32()
        this.ascSort = stream.unpackBoolean()
        run {
            val rootNode_ = NodePosition(0)
            this.rootNodePosition = rootNode_.fromBytes(stream)
        }
        return this
    }

    fun nextIndexNodeId(): Int {
        return nodesCount
    }

}
