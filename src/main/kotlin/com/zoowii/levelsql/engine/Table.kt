package com.zoowii.levelsql.engine

import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.exceptions.IndexException
import com.zoowii.levelsql.engine.index.IndexLeafNodeValue
import com.zoowii.levelsql.engine.index.IndexNodeValue
import com.zoowii.levelsql.engine.index.IndexTree
import com.zoowii.levelsql.engine.store.RowId
import com.zoowii.levelsql.engine.utils.KeyCondition
import java.util.concurrent.atomic.AtomicLong

class Table(val db: Database, val tblName: String, val nodeBytesSize: Int=1024*16, val treeDegree: Int = 100) {
    // TODO: table要支持多个索引，每个索引可能使用多个字段

    private val tree = IndexTree(db.store, "db_${db.dbName}_table_${tblName}_primary_index",
            nodeBytesSize, treeDegree, true)

    private val lastRowIdGen: AtomicLong = AtomicLong(0) // TODO: 从本地表的元信息文件中读写出来

    private fun nextRowId(): RowId {
        return RowId.fromLong(lastRowIdGen.getAndIncrement())
    }

    init {
        tree.initTree()
    }


    fun rawInsert(key: ByteArray, record: ByteArray) {
        val nextRowId = nextRowId()
        tree.addKeyValue(IndexLeafNodeValue(nextRowId, key, record))
    }

    fun rawUpdate(rowId: RowId, key: ByteArray, record: ByteArray) {
        try {
            tree.replaceKeyValue(IndexLeafNodeValue(rowId, key, record))
        }catch(e: IndexException) {
            throw DbException(e.message!!)
        }
    }

    fun rawDelete(key: ByteArray, rowId: RowId) {
        val nodeAndPos = tree.findLeafNodeByKeyAndRowId(key, rowId)
        if(nodeAndPos==null) {
            throw DbException("record not found for delete")
        }
        tree.deleteByKeyAndRowId(key, rowId)
    }

    fun rawGet(key: ByteArray): IndexLeafNodeValue? {
        val (nodeAndPos, isNewNode) = tree.findIndex(key, false)
        if(nodeAndPos==null || isNewNode) {
            return null
        }
        return nodeAndPos.node.values[nodeAndPos.indexInNode]
    }

    // raw find range by condition
    fun rawFind(condition: KeyCondition): List<IndexLeafNodeValue> {
        val first = tree.seekByCondition(condition) ?: return listOf()
        val mutableResult = mutableListOf<IndexLeafNodeValue>()
        var cur: IndexNodeValue? = first
        while(cur!=null) {
            val record = cur.leafRecord()
            if(condition.match(record.key)) {
                mutableResult += record
                cur = tree.nextRecordPosition(cur)
            } else {
                break
            }
        }
        return mutableResult.toList()
    }

    fun validate(): Boolean {
        return tree.validate()
    }

    fun toFullTreeString(): String {
        return tree.toFullTreeString()
    }

}