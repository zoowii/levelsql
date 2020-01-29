package com.zoowii.levelsql.engine.index

import com.zoowii.levelsql.engine.store.BooleanFromBytes
import com.zoowii.levelsql.engine.store.Int32FromBytes
import com.zoowii.levelsql.engine.store.StoreSerializable
import com.zoowii.levelsql.engine.store.toBytes
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

    override fun fromBytes(bytes: ByteArray): Pair<IndexMetaNode, ByteArray> {
        var remaining: ByteArray = bytes
        run {
            val p1 = Int32FromBytes(remaining)
            this.nodesCount = p1.first
            remaining = p1.second
        }
        run {
            val p2 = Int32FromBytes(remaining)
            this.nodeBytesSize = p2.first
            remaining = p2.second
        }
        run {
            val p = Int32FromBytes(remaining)
            this.nodeSubMaxCount = p.first
            remaining = p.second
        }
        run {
            val p = BooleanFromBytes(remaining)
            this.ascSort = p.first
            remaining = p.second
        }
        run {
            val rootNode_ = NodePosition(0)
            val p = rootNode_.fromBytes(remaining)
            this.rootNodePosition = rootNode_
            remaining = p.second
        }
        return Pair(this, remaining)
    }

    fun nextIndexNodeId(): Int {
        return nodesCount
    }

}
