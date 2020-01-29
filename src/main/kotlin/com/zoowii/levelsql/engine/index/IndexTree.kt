package com.zoowii.levelsql.engine.index

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import com.alibaba.fastjson.serializer.SerializerFeature
import com.zoowii.levelsql.engine.exceptions.IndexException
import com.zoowii.levelsql.engine.store.*
import com.zoowii.levelsql.engine.utils.*

// 允许key重复
// m阶B+树每个索引节点有m-1个子节点，叶子节点有m个元素
data class IndexTree(val store: IStore, var indexUniqueName: String, var nodeBytesSize: Int, val treeDegree: Int, var ascSort: Boolean) {
    // 虚拟file存储于leveldb中，一个file就是一个key->value
    // index dir一个专门文件是固定大小的header中存储index文件元信息，包括nodes数量，node大小，B+树阶数，排序顺序等，这个专门作为一个独立的file存储，方便修改
    // 然后是若干个index node，每个index node都有一个本index内自增的id, 每个都是一个独立的file
    // 每个index node头部固定大小的header是本节点的元信息，包含类型，元素数量，同级下一个节点的位置等，如果是非叶节点，包含多个指向子节点的位置，如果是叶节点，包含多个value
    // B+树每一层预分配好前2层的位置，从而方便避免修改parentNodeId，以及方便遍历
    private val log = logger()

    var metaNode: IndexMetaNode? = null
    private var nodeSubMaxCount: Int = treeDegree

    fun initTree() {
        val (metaNode, isNewMetaNode) = getMetaInfo()
        this.metaNode = metaNode
        if (isNewMetaNode) {
            // 新的meta节点需要初始化
            this.ascSort = metaNode.ascSort
            this.nodeBytesSize = metaNode.nodeBytesSize
            this.nodeSubMaxCount = metaNode.nodeSubMaxCount
            if (metaNode.rootNodePosition.offset < 0) {
                val nextNodeId = metaNode.nextIndexNodeId()
                val rootNode = IndexNode(nextNodeId, -1, null, null, nodeBytesSize, nodeSubMaxCount, true) // 空树的根节点是叶子节点
                // save root node
                saveIndexNode(rootNode)
                // update meta info node
                metaNode.rootNodePosition.offset = nextNodeId
                metaNode.nodesCount += 1
                saveMetaInfo()
            }
        }
    }

    /**
     * 叶子节点最大元素个数
     */
    fun leafNodeDegree(): Int {
        return treeDegree - 1
    }

    fun validate(): Boolean {
        // 判断是否是合法的B+树，也即是 max(left) < key <= min(right) 并且 0< nodeKeysCount <= innerNodeDegree, 0< leafValuesCount < leafNodeDegree
        val usingMetaNode = this.metaNode ?: return false
        fun validateNode(node: IndexNode): Boolean {
            if (node.leaf) {
                if (node.values.isEmpty() || node.values.size > this.leafNodeDegree()) {
                    return false
                }
                return true
            } else {
                if (node.nodeKeys.isEmpty() || node.nodeKeys.size > nodeSubMaxCount)
                    return false
                if (node.nodeKeys.size + 1 != node.subNodes.size)
                    return false
                for (i in 0 until node.nodeKeys.size) {
                    val nodeKey = node.nodeKeys[i]
                    val leftNodeId = node.subNodes[i].offset
                    val rightNodeId = node.subNodes[i + 1].offset
                    val leftNode = getIndexNode(leftNodeId) ?: return false
                    val rightNode = getIndexNode(rightNodeId) ?: return false
                    // validate leftNodeId and rightNodeId
                    assert(leftNode.rightNodeId == rightNodeId)
                    assert(rightNode.leftNodeId == leftNodeId)
                    val leftMaxKey = leftNode.rightKey()
                    val rightMinKey = rightNode.leftKey()
                    if (compareKey(leftMaxKey, nodeKey) >= 0)
                        return false
                    if (compareKey(nodeKey, rightMinKey) > 0)
                        return false
                    return true
                }
                return true
            }
        }

        val rootNode = getIndexNode(usingMetaNode.rootNodePosition.offset) ?: return false
        return validateNode(rootNode)
    }

    fun toFullTreeString(): String {
        val usingMetaNode = this.metaNode ?: return "not init"
        fun parseNodeToJson(node: IndexNode): JSONObject {
            val data = JSONObject()
            data["id"] = node.nodeId
            data["parent_id"] = node.parentNodeId
            data["left"] = node.leftNodeId
            data["right"] = node.rightNodeId
            if (node.leaf) {
                data["leaf"] = true
                data["size"] = node.values.size
                data["values"] = node.values.map { it.toString() }
            } else {
                data["size"] = node.subNodes.size
                data["keys"] = node.nodeKeys.map { bytesToHex(it) }
                val subNodes = mutableListOf<JSONObject>()
                for (subNode in node.subNodes) {
                    val subN = getIndexNode(subNode.offset)
                    if (subN != null) {
                        subNodes += parseNodeToJson(subN)
                    }
                }
                data["subNodes"] = subNodes
            }
            return data
        }

        val rootNode = getIndexNode(usingMetaNode.rootNodePosition.offset) ?: return "invalid tree"
        val rootNodeJson = parseNodeToJson(rootNode)
        return JSON.toJSONString(rootNodeJson, SerializerFeature.PrettyFormat)
    }

    private val metaInfoKey = StoreKey("${indexUniqueName}_metainfo", 0)

    // return (metaNode, isNewNode)
    fun getMetaInfo(): Pair<IndexMetaNode, Boolean> {
        // 获取本索引的元信息
        val metaInfoBytes = store.get(metaInfoKey)
        if (metaInfoBytes == null) {
            return Pair(IndexMetaNode(0, nodeBytesSize, nodeSubMaxCount, ascSort, NodePosition(-1)), true)
        } else {
            val metaNode_ = IndexMetaNode(0, nodeBytesSize, nodeSubMaxCount, ascSort, NodePosition(-1))
            val (metaNode, _) = metaNode_.fromBytes(metaInfoBytes)
            return Pair(metaNode, false)
        }
    }

    fun saveMetaInfo() {
        metaNode?.let { store.put(metaInfoKey, it.toBytes()) }
    }

    fun indexNodeStoreKey(nodeId: Int): StoreKey {
        return StoreKey(indexUniqueName, nodeId.toLong())
    }

    fun saveIndexNode(node: IndexNode) {
        val nodeBytes = node.toBytes()
        store.put(indexNodeStoreKey(node.nodeId), nodeBytes)
    }

    fun getIndexNode(nodeId: Int): IndexNode? {
        val nodeBytes = store.get(indexNodeStoreKey(nodeId))
        if (nodeBytes == null) {
            return null
        } else {
            val node = IndexNode(nodeId, -1, null, null, nodeBytesSize, nodeSubMaxCount, false)
            val p = node.fromBytes(nodeBytes)
            return p.first
        }
    }

    // TODO: 删除逻辑中删除叶子节点和删除索引节点的内容的逻辑需要统一下，需要在IndexNode中包装操作的方法

    /**
     * 修改各上级索引节点的nodeKey，把oldNodeKey替换为newNodeKey
     */
    private fun swapParentNodeKey(parentNodeId: Int, oldNodeKey: ByteArray, newNodeKey: ByteArray) {
        var curNodeId = parentNodeId
        while (curNodeId >= 0) {
            val node = getIndexNode(curNodeId) ?: return
            // 这里要找<=oldNodeKey的，如果等于就替换，否则不替换但是向上级传播
            val nodeKeyIndex = node.indexOfNodeKeyLE(oldNodeKey)
            if (nodeKeyIndex < 0) {
                // 可能是父级的第一个节点
                curNodeId = node.parentNodeId
                continue
            }
            if (compareNodeKey(node.nodeKeys[nodeKeyIndex], oldNodeKey) == 0) {
                log.debug("swaping inner node {} key {} to {}", curNodeId, nodeKeyIndex, newNodeKey)
                node.swapNodeKey(nodeKeyIndex, newNodeKey)
                saveIndexNode(node)
            }
            if (nodeKeyIndex > 0) return
            curNodeId = node.parentNodeId
        }
    }

    @Throws(IndexException::class)
    fun deleteByKeyAndRowId(key: ByteArray, rowId: RowId) {
        log.debug("deleteByKeyAndRowId ${Int32FromBytes(key).first} ${rowId.longValue()}")
        // 在B+树中根据key删除某条记录
        // 1. 先在找到的叶子节点中删除这个记录和对应的各上级的key，如果删除后节点key数量 >= Math.ceil(degree-1)/2-1，操作结束
        // 2. 如果兄弟节点（同一个父节点的左或右兄弟节点，这里采用左节点，这样方便从左节点借来的节点直接替换到父节点中）的key数量超过Math.ceil(degree-1)/2-1，则移动一个多余的记录到本节点，同时借到的节点对应的key(如果是leaf value则是key，如果是inner node则是subNode的左侧key)插入到父节点合适位置最左位置，原来父节点对应位置的key要删除
        // 3. 如果兄弟节点的key数量小于Math.ceil(degree-1)/2-1，则合并两个节点，并删除父节点中多余的key(当前节点和兄弟节点中间对应的父节点的key)。将当前节点指针设置为父节点
        // 4. 如果内部节点的key数量 >= Math.ceil(degree-1)/2-1,操作结束
        // 5. 如果兄弟节点的key数量>=Math.ceil(degree-1)/2-1, 父节点的key下移到当前节点，兄弟节点多余的key上浮1个到父节点，删除结束，否则继续
        // 6. 把当前节点，兄弟节点，和父节点下移的key合并成新节点。将当前节点指针设置为父节点，重复步骤4直到没有了父节点
        if(metaNode==null) {
            throw IndexException("meta node not set yet")
        }
        val nodeAndPos = findLeafNodeByKeyAndRowId(key, rowId) ?: return
        val foundLeafNode = nodeAndPos.node // 找到key所在叶子节点
        log.debug("found leaf node {}", foundLeafNode.nodeId)
        val foundIndexInLeafNode = nodeAndPos.indexInNode

        foundLeafNode.values = foundLeafNode.values.removeIndex(foundIndexInLeafNode)
        saveIndexNode(foundLeafNode)
        // TODO: 如果是删除的本叶子节点的最左值，则上级父节点的nodeKey也要改，并依次向上级变化
        if (foundIndexInLeafNode == 0 && foundLeafNode.values.size > 0) {
            // TODO: foundLeafNode的values本来只有一个值并且在本次删除后就没了的情况还没考虑
            swapParentNodeKey(foundLeafNode.parentNodeId, key, foundLeafNode.values[0].key)
        }
        if (foundLeafNode.isOverMinDegree())
            return
        if (foundLeafNode.leftNodeId == null)
            return
        if (foundLeafNode.leftNodeId!! < 0) {
            return // 如果是最左叶子节点删除数据，暂时不处理
        }
        var curNode = foundLeafNode
        do {
            val leftLeafNode = getIndexNode(curNode.leftNodeId!!)!!
            if (curNode.parentNodeId < 0) {
                break
            }
            val parentNode = getIndexNode(curNode.parentNodeId) ?: break
            val parentNodeKeyToSwapKey = curNode.leftKey() // foundLeafNode在父节点中对应的nodeKey
            val parentNodeKeyIndexToSwapKey = parentNode.indexOfNodeKey(parentNodeKeyToSwapKey)
            if (leftLeafNode.isOverMinDegree()) {
                // 进入第2步
                if (curNode.leaf) {
                    val toMoveValue = leftLeafNode.values[leftLeafNode.values.size - 1]
                    leftLeafNode.values = leftLeafNode.values.subList(0, leftLeafNode.values.size - 1)
                    saveIndexNode(leftLeafNode)
                    curNode.values = listOf(toMoveValue) + curNode.values
                    saveIndexNode(curNode)
                    // 修改leftLeafNode和foundLeafNode的父节点对应的nodeKey替换为toMoveValue对应的key
                    parentNode.swapNodeKey(parentNodeKeyIndexToSwapKey, toMoveValue.key)
                } else {
                    val toMoveSubNodePos = leftLeafNode.subNodes[leftLeafNode.subNodes.size - 1]
                    val toMoveSubNodeKey = leftLeafNode.nodeKeys[leftLeafNode.nodeKeys.size - 1]
                    leftLeafNode.subNodes = leftLeafNode.subNodes.subList(0, leftLeafNode.subNodes.size - 1)
                    saveIndexNode(leftLeafNode)
                    curNode.subNodes = listOf(toMoveSubNodePos) + curNode.subNodes
                    curNode.nodeKeys = listOf(toMoveSubNodeKey) + curNode.nodeKeys
                    saveIndexNode(curNode)
                    // 修改leftLeafNode和foundLeafNode的父节点对应的nodeKey替换为toMoveValue对应的key
                    parentNode.swapNodeKey(parentNodeKeyIndexToSwapKey, toMoveSubNodeKey)
                }
                saveIndexNode(parentNode)
                curNode = parentNode
            } else {
                // 进入第3步
                if (curNode.leaf) {
                    leftLeafNode.values += curNode.values
                } else {
                    leftLeafNode.subNodes += curNode.subNodes
                    leftLeafNode.nodeKeys += curNode.nodeKeys
                }
                leftLeafNode.rightNodeId = curNode.rightNodeId
                if (curNode.rightNodeId != null) {
                    val rightLeafNode = getIndexNode(curNode.rightNodeId!!)
                    rightLeafNode?.leftNodeId = leftLeafNode.nodeId
                    saveIndexNode(rightLeafNode!!)
                }
                parentNode.removeSubNodeAndNodeKey(parentNodeKeyIndexToSwapKey + 1)
                saveIndexNode(leftLeafNode)
                saveIndexNode(curNode)
                saveIndexNode(parentNode)
                curNode = parentNode
            }
        } while (true)
    }

    fun propagateNodeKeyChange(curNode: IndexNode, oldNodeLeftNodeKey: ByteArray) {
        // curNode的最左key变更了，可能需要传播到上级节点修改对应位置的nodeKey
        if (curNode.parentNodeId < 0) {
            return
        }
        val parentNode = getIndexNode(curNode.parentNodeId) ?: return
        val nodeIndex = parentNode.indexOfNodeKey(oldNodeLeftNodeKey)
        if (nodeIndex < 0) {
            return
        }
        val newKey = curNode.leftKey()
        if (compareKey(parentNode.nodeKeys[nodeIndex], newKey) == 0) {
            return
        }
        parentNode.swapNodeKey(nodeIndex, newKey)
        saveIndexNode(parentNode)
        propagateNodeKeyChange(parentNode, oldNodeLeftNodeKey)
    }

    @Throws(IndexException::class)
    fun replaceKeyValue(value: IndexLeafNodeValue) {
        // 在B+书中更新记录, value中包含了row id和key
        // 根据rowId查找并替换节点
        val nodeAndPos = findLeafNodeByKeyAndRowId(value.key, value.rowId)
                ?: throw IndexException("record not found for update")
        nodeAndPos.node.setLeafValue(nodeAndPos.indexInNode, value)
        saveIndexNode(nodeAndPos.node)
    }

    @Throws(IndexException::class)
    fun addKeyValue(value: IndexLeafNodeValue) {
        // 在B+树中插入新记录，value包含row id要求每次都不一样,一样的不重复插入
        val key = value.key
        // 找到>=key的叶子节点的位置，没有就是根节点，插入新的叶子节点，否则从找到位置的叶子节点开始操作
        // 1. 先在叶子节点插入数据，
        // 2. 如果叶子节点满了，拆分成左右两个叶子节点，移动一半数据到右节点，两个新节点都作为原来父节点的子节点，增加新key到父节点,拆分后移动指针到上一级父节点继续操作
        // 3. 到内部节点后，如果这个内部节点没满，操作结束，如果这个内部节点满了，拆分这个内部节点成左右两个内部节点，移动原来内部节点的中间key到父节点，两个新节点都作为原来父节点的子节点，移动指针到上级父节点继续操作
        // 4. 如果指针到了根节点或者没有上一级节点了，操作结束
        // 过程中涉及元信息变化的比如节点数增加的，需要修改meta node
        val usingMetanode = metaNode ?: throw IndexException("meta node not set yet")
        var curNodeId = usingMetanode.rootNodePosition.offset
        var curNode: IndexNode
        val (nodeAndPos, isNewNode) = findIndex(key, true)
        if (nodeAndPos != null) {
            curNodeId = nodeAndPos.node.nodeId
        }
        val nodeOption = getIndexNode(curNodeId) ?: throw IndexException("node not found")
        curNode = nodeOption
        if (!curNode.leaf)
            throw IndexException("only can insert into leaf node in B+ tree")

        // 插入数值时,findIndex出的结果，如果本叶子节点满了，下一个叶子节点没满，则插入下一个叶子节点
//        log.debug("found leaf node by index #{}", curNodeId)

        // 这里用if而不是while，从而避免太多次IO，而是做一次拆分
        if (curNode.isFullsizeLeafNode()) {
            // 如果要插入的key值大于curNode.values最大key值时可以尝试向右插入
            if (compareKey(key, curNode.values[curNode.values.size - 1].key) > 0) {
                val curRightNodeId = curNode.rightNodeId
                if (curRightNodeId != null) {
                    curNodeId = curRightNodeId
                    curNode = getIndexNode(curNodeId) ?: throw IndexException("node not found")
                }
//                log.debug("changing to insert in leaf node #{}", curNodeId)
            }
        }

        if (curNode.values.isEmpty()) {
            // 第一个插入的根节点
            curNode.addLeafValue(value)
            saveIndexNode(curNode)
            return
        }
        // 如果是旧节点, 插入应该插入的位置，否则直接追加
        // 叶子节点插入数据只插入values
        val curNodeOldLeftNodeKey = curNode.values[0].key
        val isInsertInFirst = compareKey(key, curNodeOldLeftNodeKey) < 0 // 是否是插入到节点第一个位置
        curNode.addLeafValue(value)
        saveIndexNode(curNode)
        // 进入步骤2
        if (!curNode.isOverSizeLeafNode()) {
            // 叶子节点没满，直接使用这个节点
            if (isInsertInFirst && curNode.hasLeftNode()) {
                // 如果是插入到叶子节点的开头并且本叶子节点不是第一个叶子节点，则需要更新上级节点的nodeKey
                propagateNodeKeyChange(curNode, curNodeOldLeftNodeKey)
            }
            return
        }
        // 拆分叶子节点，以及其他步骤
        val moveToLeftValuesCount = curNode.values.size / 2
        val leftValues = curNode.values.subList(0, moveToLeftValuesCount)
        val rightValues = curNode.values.subList(moveToLeftValuesCount, curNode.values.size)
        val leftNodeId = metaNode!!.nextIndexNodeId()
        metaNode!!.nodesCount += 1
        val rightNodeId = metaNode!!.nextIndexNodeId()
        metaNode!!.nodesCount += 1
        val leafMiddleKey = curNode.values[moveToLeftValuesCount].key
        // 以上新sub node id的顺序不有序

        val parentNode = getIndexNode(curNode.parentNodeId)
        // 拆分或者废弃节点的时候，如果一个节点被废弃了，那原来指向这个节点的节点要修改。所以拆分节点的时候尽量原地拆分，保留其中一个节点。或者需要注意是否需要修改curNode.leftNode.rightNodeId和curNode.rightNode.leftNodeId
        // curNode因为可能作为2个新叶子节点的父节点，所以不能原地拆分。或者需要新建父节点，curNode原地拆分. 目前采用把curNode作为父节点的方案

        if (curNode.leftNodeId != null) {
            val curNodeLeftNode = getIndexNode(curNode.leftNodeId!!)
            if (curNodeLeftNode != null) {
                curNodeLeftNode.rightNodeId = leftNodeId
                saveIndexNode(curNodeLeftNode)
            }
        }
        if (curNode.rightNodeId != null) {
            val curNodeRightNode = getIndexNode(curNode.rightNodeId!!)
            if (curNodeRightNode != null) {
                curNodeRightNode.leftNodeId = rightNodeId
                saveIndexNode(curNodeRightNode)
            }
        }

        val leftNode = IndexNode(leftNodeId, if (parentNode == null) curNodeId else curNode.parentNodeId, curNode.leftNodeId, rightNodeId, nodeBytesSize, nodeSubMaxCount, true)
        leftNode.values = leftValues
        val rightNode = IndexNode(rightNodeId, if (parentNode == null) curNodeId else curNode.parentNodeId, leftNodeId, curNode.rightNodeId, nodeBytesSize, nodeSubMaxCount, true)
        rightNode.values = rightValues
        saveIndexNode(leftNode)
        saveIndexNode(rightNode)
        if (parentNode == null) {
            // 没找到父节点，可能curNode已经是根节点（同时是叶子节点）,需要拆分根节点
            curNode.subNodes = listOf(NodePosition(leftNode.nodeId), NodePosition(rightNode.nodeId))
            curNode.nodeKeys = listOf(rightNode.leftKey())
            curNode.values = listOf()
            curNode.leaf = false
            saveIndexNode(curNode)
        } else {
            // 本叶子节点移除（以后可以优化成作为左节点），父节点中在原来位置插入两个生成的新节点
            val indexInParent = parentNode.subNodes.indexOfFirst { it.offset == curNodeId }
            if (indexInParent < 0)
                throw IndexException("invalid tree of parent node")
            // 中间node key上升到父节点的时候要插入到合适的位置，而不是随便插入
            parentNode.nodeKeys = parentNode.nodeKeys.subList(0, indexInParent) + leafMiddleKey + parentNode.nodeKeys.subList(indexInParent, parentNode.nodeKeys.size)
            parentNode.subNodes = parentNode.subNodes.subList(0, indexInParent) + NodePosition(leftNode.nodeId) + NodePosition(rightNode.nodeId) + parentNode.subNodes.subList(indexInParent + 1, parentNode.subNodes.size)
            saveIndexNode(parentNode)
            // 移动指针到父节点
            curNode = parentNode
            curNodeId = parentNode.nodeId
        }
        // 进入步骤3
        while (curNode.subNodes.size > nodeSubMaxCount) {
            val oldSubNodesCount = curNode.subNodes.size
            val oldParentNodeId = curNode.parentNodeId
            val leftSubNodesCount = oldSubNodesCount / 2
            val leftSubNodes = curNode.subNodes.subList(0, leftSubNodesCount)
            val leftNodeKeys = curNode.nodeKeys.subList(0, leftSubNodesCount - 1)
            val rightSubNodes = curNode.subNodes.subList(leftSubNodesCount, oldSubNodesCount)
            val rightNodeKeys = curNode.nodeKeys.subList(leftSubNodesCount, curNode.nodeKeys.size)
            val middleNodeKey = curNode.nodeKeys[leftSubNodesCount - 1]
            // curNode原地作为新的左节点，所以接下来需要新建一个右节点
            curNode.subNodes = leftSubNodes
            curNode.nodeKeys = leftNodeKeys
            val newRightNodeId = metaNode!!.nextIndexNodeId()
            metaNode!!.nodesCount += 1
            val newRightNode = IndexNode(newRightNodeId, oldParentNodeId, curNodeId, curNode.rightNodeId, nodeBytesSize, nodeSubMaxCount, false)
            newRightNode.subNodes = rightSubNodes
            newRightNode.nodeKeys = rightNodeKeys
            curNode.rightNodeId = newRightNodeId
            // rightSubNodes的parentNodeId要修改
            for (subNode in newRightNode.subNodes) {
                val node = getIndexNode(subNode.offset)
                if (node == null) {
                    throw IndexException("can't find inner node ${subNode.offset}")
                }
                node.parentNodeId = newRightNodeId
                saveIndexNode(node)
            }
            saveIndexNode(curNode)
            saveIndexNode(newRightNode)
            val oldParentNode = getIndexNode(oldParentNodeId)
            if (oldParentNode == null) {
                // 如果上级不存在父节点，就新建一个父节点用来存储上溢的middleNodeKey
                val newRootNodeId = usingMetanode.nextIndexNodeId()
                usingMetanode.nodesCount += 1
                val newRootNode = IndexNode(newRootNodeId, -1, null, null, nodeBytesSize, nodeSubMaxCount, false)
                newRootNode.subNodes = listOf(NodePosition(curNode.nodeId), NodePosition(newRightNode.nodeId))
                newRootNode.nodeKeys = listOf(middleNodeKey)
                saveIndexNode(newRootNode)
                curNode.parentNodeId = newRootNodeId
                saveIndexNode(curNode)
                newRightNode.parentNodeId = newRootNodeId
                saveIndexNode(newRightNode)
                usingMetanode.rootNodePosition.offset = newRootNodeId
                saveMetaInfo()
                break
            }
            // 看curNode是parentNode的第几个subNode(k)，然后在parentNode的nodeKeys的k位置插入middleNodeKey
            val indexInParent = oldParentNode.subNodes.indexOfFirst { it.offset == curNodeId }
            if (indexInParent < 0)
                throw IndexException("invalid tree of parent node")
            // 中间node key上升到父节点的时候要插入到合适的位置，而不是随便插入
            if (indexInParent < oldParentNode.nodeKeys.size) {
                oldParentNode.nodeKeys = oldParentNode.nodeKeys.subList(0, indexInParent) + middleNodeKey + oldParentNode.nodeKeys.subList(indexInParent, oldParentNode.nodeKeys.size)
            } else {
                oldParentNode.nodeKeys = oldParentNode.nodeKeys + listOf(middleNodeKey)
            }
            if (indexInParent + 1 < oldParentNode.subNodes.size) {
                oldParentNode.subNodes = oldParentNode.subNodes.subList(0, indexInParent + 1) + NodePosition(newRightNode.nodeId) + oldParentNode.subNodes.subList(indexInParent + 1, oldParentNode.subNodes.size)
            } else {
                oldParentNode.subNodes = oldParentNode.subNodes + listOf(NodePosition(newRightNode.nodeId))
            }
            saveIndexNode(oldParentNode)
            curNodeId = oldParentNodeId
            curNode = oldParentNode
        }
        // TODO: 受影响的nodes可以不在本级操作时立刻写回磁盘，而可以在到上级时确保本节点已经处理完后再saveIndexNode

        // 保存元信息节点
        saveMetaInfo()
    }

    fun compareKey(key1: ByteArray, key2: ByteArray): Int {
        val less = compareNodeKey(key1, key2)
        if (less == 0)
            return 0
        if (ascSort) {
            return less
        } else {
            return -less
        }
    }

    // 把list当成b+树索引节点找到在哪个子树(node.subNodes中的索引，而不是list中的索引)去搜索满足cond的子节点
    fun bplusTreeSearchByCondition(nodeKeys: List<ByteArray>, cond: KeyCondition): Int {
        // 因为索引节点已经都加载到内存暂时不是性能瓶颈，目前用遍历搜索而不是二叉搜索
        for (i in 0..nodeKeys.size) {
            val rangeBegin = if (i == 0) null else nodeKeys[i - 1]
            val rangeEnd = if (i == nodeKeys.size) null else nodeKeys[i]
            if (cond.acceptRange(rangeBegin, rangeEnd)) {
                return i
            }
        }
        return nodeKeys.size
    }

    // 在list中二叉搜索到满足(cond, key)的最左值
    fun <T> binarySearchByCondition(list: List<T>, keyFetcher: (T) -> ByteArray, cond: KeyCondition): Int? {
        // TODO: ascSort需要和cond合并。因为索引排序方式不一定从小到大。或者cond需要接收ascSort作为构造函数的参数
        val matchedIndexes = mutableListOf<Int>()
        fun searcher(list: List<T>, keyFetcher: (T) -> ByteArray, minIndex: Int, maxIndex: Int): Int? {
            if (minIndex > maxIndex)
                return null
            val minKey = keyFetcher(list[minIndex])
            if (cond.match(minKey)) {
                matchedIndexes += minIndex
                return minIndex
            }
            if (!cond.greaterMayMatch(minKey)) {
                return null
            }
            val mid = (minIndex + maxIndex) / 2
            val midVal = list[mid]
            val midKey = keyFetcher(midVal)
            if (cond.match(midKey)) {
                matchedIndexes += mid
                // midKey满足的时候，尝试看看mid左侧的值(左侧值可能和mid值一样)是否也能满足. 这种情况需要记录下找到满足的keys的列表然后再去向左搜索。最后结果就是keys中最左值
                searcher(list, keyFetcher, minIndex, mid - 1)
                return mid
            }
            if (cond.lessMayMatch(midKey)) {
                return searcher(list, keyFetcher, minIndex, mid - 1)
            }
            if (cond.greaterMayMatch(midKey)) {
                return searcher(list, keyFetcher, mid + 1, maxIndex)
            }
            return null
        }

        val searched = searcher(list, keyFetcher, 0, list.size - 1)
        if (searched == null) {
            // TODO: searched为null时，可以考虑下一个同级节点来查找。在 大于条件查找并且第一个叶子节点不满足条件的时候可能用到
            return null
        }
        if (matchedIndexes.isEmpty()) {
            return searched
        }
        return matchedIndexes.min()
    }

    // @return pairOf(index, needInsert)
    fun <T, K> binarySearchOrNewPosition(list: List<T>, keyFetcher: (T) -> K, key: K, comparator: (K, K) -> Int): Pair<Int, Boolean> {
        // 二分查找元素或者没找到返回应该插入的位置(插入后保持有序)
        fun searcher(list: List<T>, keyFetcher: (T) -> K, key: K, comparator: (K, K) -> Int, minIndex: Int, maxIndex: Int): Pair<Int, Boolean> {
            if (minIndex > maxIndex)
                return Pair(list.size, true)
            if (comparator(key, keyFetcher(list[minIndex])) < 0)
                return Pair(minIndex, true)
            if (comparator(key, keyFetcher(list[maxIndex])) > 0)
                return Pair(maxIndex + 1, true)
            val mid = (minIndex + maxIndex) / 2
            val midVal = list[mid]
            val midKey = keyFetcher(midVal)
            val compareResult = comparator(key, midKey)
            return when {
                compareResult == 0 -> Pair(mid, false)
                compareResult > 0 -> searcher(list, keyFetcher, key, comparator, mid + 1, maxIndex)
                compareResult < 0 -> searcher(list, keyFetcher, key, comparator, minIndex, mid - 1)
                else -> Pair(list.size, true)
            }
        }
        return searcher(list, keyFetcher, key, comparator, 0, list.size - 1)
    }

    // 前一个record记录的位置
    fun prevRecordPosition(curNodeAndPos: IndexNodeValue): IndexNodeValue? {
        if (curNodeAndPos.indexInNode > 0) {
            return IndexNodeValue(curNodeAndPos.node, curNodeAndPos.indexInNode - 1)
        }
        // 本叶子节点当前位置是0，需要跳到左侧节点继续遍历
        var cur: IndexNodeValue? = curNodeAndPos
        while (cur != null) {
            val leftNodeId = cur.node.leftNodeId ?: return null
            val leftNode = getIndexNode(leftNodeId) ?: throw IndexException("can't find node $leftNodeId")
            if (leftNode.values.isNotEmpty()) {
                return IndexNodeValue(leftNode, leftNode.values.size - 1)
            }
            cur = IndexNodeValue(leftNode, 0)
        }
        return null
    }

    // 后一个record记录的位置
    fun nextRecordPosition(curNodeAndPos: IndexNodeValue): IndexNodeValue? {
        if (curNodeAndPos.indexInNode < curNodeAndPos.node.values.size - 1) {
            return IndexNodeValue(curNodeAndPos.node, curNodeAndPos.indexInNode + 1)
        }
        // 本叶子节点各records已经遍历完成，需要跳到右侧节点继续遍历
        val rightNodeId = curNodeAndPos.node.rightNodeId ?: return null
        val rightNode = getIndexNode(rightNodeId) ?: throw IndexException("can't find node $rightNodeId")
        return IndexNodeValue(rightNode, 0)
    }

    // 根据{key}和{rowId}找到叶子节点和record在叶子节点中的位置
    fun findLeafNodeByKeyAndRowId(key: ByteArray, rowId: RowId): IndexNodeValue? {
        // 先找到满足key条件的第一个叶子节点和在叶子节点中的位置
        val (nodeAndPos, isNewPosition) = findIndex(key)
        if (nodeAndPos == null || isNewPosition) {
            return null
        }
        var curNodeAndPos: IndexNodeValue? = nodeAndPos
        while (curNodeAndPos != null) {
            if (curNodeAndPos.indexInNode < curNodeAndPos.node.values.size
                    && curNodeAndPos.node.values[curNodeAndPos.indexInNode].rowId.equals(rowId)) {
                break
            }
            curNodeAndPos = nextRecordPosition(curNodeAndPos)
        }
        return curNodeAndPos
    }

    // TODO: seekByCondition和findIndex的逻辑合并
    // 条件搜索找到第一个满足condition(value)条件的record
    fun seekByCondition(condition: KeyCondition): IndexNodeValue? {
        // 按B+树的方式查找到满足条件的最左索引所在的叶子节点
        val usingMetanode = metaNode ?: return null
        var curNodeId = usingMetanode.rootNodePosition.offset
        var curNode: IndexNode
        while (true) {
            val nodeOption = getIndexNode(curNodeId) ?: return null
            curNode = nodeOption
            if (curNode.leaf) {
                // 在叶子节点中找到数据节点和位置
                val searchResult = binarySearchByCondition(curNode.values, IndexLeafNodeValue::key, condition)
                if (searchResult == null) {
                    if (condition.acceptGreaterKey()) {
                        // 可能下一个叶子节点才包含数据
                        curNodeId = curNode.rightNodeId ?: return null
                        curNode = getIndexNode(curNodeId) ?: return null
                        val nextSearchResult = binarySearchByCondition(curNode.values, IndexLeafNodeValue::key, condition)
                                ?: return null
                        return IndexNodeValue(curNode, nextSearchResult)
                    }
                    return null
                }
                return IndexNodeValue(curNode, searchResult)
            }
            if (curNode.nodeKeys.isEmpty()) {
                return null
            }
            assert(curNode.nodeKeys.isNotEmpty())
            // 在索引节点中搜索满足condition的最左节点
            val subTreeIdx = bplusTreeSearchByCondition(curNode.nodeKeys, condition)
            curNodeId = curNode.subNodes[subTreeIdx].offset
        }
    }

    // @return: (indexNodeValue, isNewPosition)
    fun findIndex(key: ByteArray, allowGreat: Boolean = false): Pair<IndexNodeValue?, Boolean> {
        // 按B+树的方式查找到满足条件的索引所在的叶子节点
        val usingMetanode = metaNode
        if (usingMetanode == null) {
            return Pair(null, true)
        }
        var curNodeId = usingMetanode.rootNodePosition.offset
        var curNode: IndexNode
        while (true) {
            val nodeOption = getIndexNode(curNodeId)
            if (nodeOption == null) {
                return Pair(null, true)
            }
            curNode = nodeOption
            if (curNode.leaf) {
                // 在叶子节点中找到数据节点和位置
                val searchResult = binarySearchOrNewPosition(curNode.values, IndexLeafNodeValue::key, key) { k1: ByteArray, k2: ByteArray ->
                    compareKey(k1, k2)
                }
                if (!allowGreat && searchResult.second)
                    return Pair(null, true)
                return Pair(IndexNodeValue(curNode, searchResult.first), searchResult.second)
            }
            if (curNode.nodeKeys.isEmpty()) {
                return Pair(null, true)
            }
            val subSearchResult = binarySearchOrNewPosition(curNode.nodeKeys, { i -> i }, key) { k1: ByteArray, k2: ByteArray ->
                compareKey(k1, k2)
            }
            if (subSearchResult.second)
                curNodeId = curNode.subNodes[subSearchResult.first].offset
            else
                curNodeId = curNode.subNodes[subSearchResult.first + 1].offset
        }
    }

}