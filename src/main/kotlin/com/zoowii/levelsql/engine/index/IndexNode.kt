package com.zoowii.levelsql.engine.index

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONArray
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zoowii.levelsql.engine.exceptions.IndexException
import com.zoowii.levelsql.engine.exceptions.SerializeException
import com.zoowii.levelsql.engine.store.*
import com.zoowii.levelsql.engine.utils.compareNodeKey
import com.zoowii.levelsql.engine.utils.removeIndex
import com.zoowii.levelsql.engine.utils.safeSlice
import java.awt.event.KeyAdapter
import java.io.ByteArrayOutputStream
import java.io.Serializable
import java.nio.charset.Charset
import java.util.*

data class IndexNode(var nodeId: Int, var parentNodeId: Int, var leftNodeId: Int?, var rightNodeId: Int?, val nodeBytesSize: Int, val nodeSubMaxCount: Int, var leaf: Boolean) : StoreSerializable<IndexNode> {
    override fun toBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(leaf.toBytes())
        out.write(nodeId.toBytes())
        out.write(parentNodeId.toBytes())
        val localLeftNodeId = leftNodeId
        if(localLeftNodeId==null) {
            out.write(0)
        } else {
            out.write(1)
            out.write(localLeftNodeId.toBytes())
        }
        val localRightNodeId = rightNodeId
        if(localRightNodeId==null) {
            out.write(0)
        } else {
            out.write(1)
            out.write(localRightNodeId.toBytes())
        }
        out.write(subNodes.size.toBytes())
        for (node in subNodes) {
            out.write(node.toBytes())
        }
        out.write(nodeKeys.size.toBytes())
        for (key in nodeKeys) {
            out.write(key.toBytes())
        }
        out.write(values.size.toBytes())
        for (value in values) {
            out.write(value.toBytes())
        }
        val resultBytes = out.toByteArray()
        if (resultBytes.size > nodeBytesSize) {
            throw SerializeException("too large index node")
        }
        return resultBytes
    }

    override fun fromBytes(bytes: ByteArray): Pair<IndexNode, ByteArray> {
        var remaining: ByteArray = bytes

        run {
            val p = BooleanFromBytes(remaining)
            this.leaf = p.first
            remaining = p.second
        }

        run {
            val p = Int32FromBytes(remaining)
            this.nodeId = p.first
            remaining = p.second
        }
        run {
            val p = Int32FromBytes(remaining)
            this.parentNodeId = p.first
            remaining = p.second
        }
        run {
            val p = BooleanFromBytes(remaining)
            if(p.first) {
                val p2 = Int32FromBytes(p.second)
                this.leftNodeId = p2.first
                remaining = p2.second
            } else {
                this.leftNodeId = null
                remaining = p.second
            }
        }
        run {
            val p = BooleanFromBytes(remaining)
            if(p.first) {
                val p2 = Int32FromBytes(p.second)
                this.rightNodeId = p2.first
                remaining = p2.second
            } else {
                this.rightNodeId = null
                remaining = p.second
            }
        }
        run {
            val subNodesCount: Int
            val p = Int32FromBytes(remaining)
            subNodesCount = p.first
            remaining = p.second
            for (i in 0 until subNodesCount) {
                val node = NodePosition(0)
                val p2 = node.fromBytes(remaining)
                remaining = p2.second
                this.subNodes += node
            }
        }
        run {
            val p = Int32FromBytes(remaining)
            val nodeKeysCount = p.first
            remaining = p.second
            for (i in 0 until nodeKeysCount) {
                val p2 = ByteArrayFromBytes(remaining)
                remaining = p2.second
                this.nodeKeys += p2.first
            }
        }
        run {
            val p = Int32FromBytes(remaining)
            val valuesCount = p.first
            remaining = p.second
            for (i in 0 until valuesCount) {
                val item = IndexLeafNodeValue(RowId(byteArrayOf()), byteArrayOf(), byteArrayOf())
                val p2 = item.fromBytes(remaining)
                remaining = p2.second
                this.values += p2.first
            }
        }
        return Pair(this, remaining)
    }

    fun leftKey(): ByteArray {
        if (leaf) {
            if (values.isEmpty())
                throw IndexException("empty leaf node")
            return values[0].key
        } else {
            if (nodeKeys.isEmpty())
                throw IndexException("empty inner node")
            return nodeKeys[0]
        }
    }

    fun rightKey(): ByteArray {
        if (leaf) {
            if (values.isEmpty())
                throw IndexException("empty leaf node")
            return values[values.size-1].key
        } else {
            if (nodeKeys.isEmpty())
                throw IndexException("empty inner node")
            return nodeKeys[nodeKeys.size-1]
        }
    }

    override fun toString(): String {
        return super.toString() + " sub nodes count ${subNodes.size} values count ${values.size}"
    }

    var subNodes: List<NodePosition> = listOf()
    // 考虑nodeKeys保存subNodes中各子树的最小key值，也就是改成nodeKeys数量比subNodes小1
    // nodeKeys数量比subNodes小于或者等于0，最小为0. nodeKeys中包含了subNodes[1:]中的最左key，叶子节点的最左value的key和内部节点的最左nodeKeys
    var nodeKeys: List<ByteArray> = listOf()
    var values: List<IndexLeafNodeValue> = listOf() // values的最大数量是树的阶数-1

    fun indexOfNodeKey(key: ByteArray): Int {
        for(i in nodeKeys.indices) {
            val item = nodeKeys[i]
            if(compareNodeKey(item, key)==0) {
                return i
            }
        }
        return -1
    }

    /**
     * 在nodeKeys中找到最后一个<=key的索引
     */
    fun indexOfNodeKeyLE(key: ByteArray): Int {
        for(i in nodeKeys.indices.reversed()) {
            val item = nodeKeys[i]
            if(compareNodeKey(item, key)<=0) {
                return i
            }
        }
        return -1
    }

    /**
     * 修改第{index}个nodeKey
     */
    fun swapNodeKey(index: Int, newNodeKey: ByteArray): IndexNode {
        if(index>=nodeKeys.size) {
            this.nodeKeys = this.nodeKeys + newNodeKey
            return this
        }
        val mutableNodeKeys = this.nodeKeys.toMutableList()
        mutableNodeKeys[index] = newNodeKey
        this.nodeKeys = mutableNodeKeys.toList()
        return this
    }

    /**
     * 删除第{nodeIndex}个subNode和对应的nodeKey
     * @param nodeIndex 子节点的索引
     */
    fun removeSubNodeAndNodeKey(nodeIndex: Int): IndexNode {
        assert(nodeIndex>=1)
        val mutableNodeKeys = this.nodeKeys.toMutableList()
        mutableNodeKeys.removeAt(nodeIndex-1)
        this.nodeKeys = mutableNodeKeys.toList()
        val mutableSubNodes = this.subNodes.toMutableList()
        mutableSubNodes.removeAt(nodeIndex)
        this.subNodes = mutableSubNodes.toList()
        return this
    }

    fun leafNodeMinDegree() = Math.ceil(nodeSubMaxCount.toDouble()-1)/2 - 1

    fun innerNodeMinDegree() = Math.ceil(nodeSubMaxCount.toDouble()-1)/2-1

    fun isOverMinDegree(): Boolean {
        if(this.leaf) {
            return this.values.size > leafNodeMinDegree()
        } else {
            return this.subNodes.size > innerNodeMinDegree()
        }
    }

    fun isOverSizeLeafNode(): Boolean {
        return leaf && values.size >= nodeSubMaxCount
    }

    fun isFullsizeLeafNode(): Boolean {
        return leaf && values.size >= (nodeSubMaxCount - 1)
    }

    fun setLeafValue(idx: Int, value: IndexLeafNodeValue) {
        this.values = values.safeSlice(0, idx) + value + values.safeSlice(idx+1, values.size)
    }

    fun addLeafValue(value: IndexLeafNodeValue) {
        val valueKey = value.key
        // 找到最后一个<=valueKey的索引
        var idxToInsertAfter: Int = -1
        for(i in values.indices.reversed()) {
            if(compareNodeKey(values[i].key, valueKey)<=0) {
                idxToInsertAfter = i
                break
            }
        }
        this.values = values.safeSlice(0, idxToInsertAfter+1) + value + values.safeSlice(idxToInsertAfter+1,values.size)
    }

    fun hasLeftNode(): Boolean {
        return leftNodeId != null
    }

    fun hasRightNode(): Boolean {
        return rightNodeId != null
    }
}
