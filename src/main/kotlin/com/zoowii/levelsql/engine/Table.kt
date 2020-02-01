package com.zoowii.levelsql.engine

import com.zoowii.levelsql.TableColumnDefinition
import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.exceptions.IndexException
import com.zoowii.levelsql.engine.index.IndexLeafNodeValue
import com.zoowii.levelsql.engine.index.IndexNodeValue
import com.zoowii.levelsql.engine.index.IndexTree
import com.zoowii.levelsql.engine.store.RowId
import com.zoowii.levelsql.engine.store.toBytes
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import com.zoowii.levelsql.engine.utils.KeyCondition
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.concurrent.atomic.AtomicLong

class Table(val db: Database, val tblName: String, val primaryKey: String, val columns: List<TableColumnDefinition>,
            val nodeBytesSize: Int=1024*16, val treeDegree: Int = 100) {

    fun metaToBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(db.dbName.toBytes())
        out.write(tblName.toBytes())
        out.write(primaryKey.toBytes())
        out.write(columns.size.toBytes())
        for(c in columns) {
            out.write(c.toBytes())
        }
        out.write(nodeBytesSize.toBytes())
        out.write(treeDegree.toBytes())
        // save secondary indexes
        out.write(secondaryIndexes.size.toBytes())
        secondaryIndexes.map {
            out.write(it.metaToBytes())
        }
        return out.toByteArray()
    }

    companion object {
        fun metaFromBytes(db: Database, stream: ByteArrayStream): Table {
            val dbName = stream.unpackString()
            val tblName = stream.unpackString()
            val primaryKey = stream.unpackString()
            val columnsCount = stream.unpackInt32()
            val columns = (0 until columnsCount).map { TableColumnDefinition.fromBytes(stream) }
            val nodeBytesSize = stream.unpackInt32()
            val treeDegree = stream.unpackInt32()
            if(dbName!=db.dbName) {
                throw IOException("conflict db name $dbName and ${db.dbName} when load table")
            }
            val table = Table(db, tblName, primaryKey, columns, nodeBytesSize, treeDegree)
            // load secondary indexes
            val secondaryIndexesCount = stream.unpackInt32()
            table.secondaryIndexes = (0 until secondaryIndexesCount).map {
                Index.metaFromBytes(table, stream)
            }
            return table
        }
    }

    // TODO: table要支持多个索引，每个索引可能使用多个字段

    // 聚集索引
    private val primaryIndex = Index(this, "${tblName}_primary_${primaryKey}", listOf(primaryKey), true, true)
    // 二级索引
    private var secondaryIndexes = listOf<Index>()

    fun openIndex(indexName: String): Index? {
        if(primaryIndex.indexName == indexName) {
            return primaryIndex
        }
        return secondaryIndexes.firstOrNull {it.indexName==indexName}
    }

    fun createIndex(indexName: String, columns: List<String>, unique: Boolean): Index {
        val existed = openIndex(indexName)
        if(existed!=null) {
            throw DbException("index name $tblName.$indexName conflict")
        }
        val index = Index(this, indexName, columns, unique, false)
        secondaryIndexes = secondaryIndexes + index
        return index
    }

    private val lastRowIdGen: AtomicLong = AtomicLong(0) // TODO: 从本地表的元信息文件中读写出来

    private fun nextRowId(): RowId {
        return RowId.fromLong(lastRowIdGen.getAndIncrement())
    }

    init {
        primaryIndex.tree.initTree()
    }


    fun rawInsert(key: ByteArray, record: ByteArray) {
        val nextRowId = nextRowId()
        primaryIndex.tree.addKeyValue(IndexLeafNodeValue(nextRowId, key, record))
    }

    fun rawUpdate(rowId: RowId, key: ByteArray, record: ByteArray) {
        try {
            primaryIndex.tree.replaceKeyValue(IndexLeafNodeValue(rowId, key, record))
        }catch(e: IndexException) {
            throw DbException(e.message!!)
        }
    }

    fun rawDelete(key: ByteArray, rowId: RowId) {
        val nodeAndPos = primaryIndex.tree.findLeafNodeByKeyAndRowId(key, rowId)
        if(nodeAndPos==null) {
            throw DbException("record not found for delete")
        }
        primaryIndex.tree.deleteByKeyAndRowId(key, rowId)
    }

    fun rawGet(key: ByteArray): IndexLeafNodeValue? {
        val (nodeAndPos, isNewNode) = primaryIndex.tree.findIndex(key, false)
        if(nodeAndPos==null || isNewNode) {
            return null
        }
        return nodeAndPos.node.values[nodeAndPos.indexInNode]
    }

    // 找到表中第一条数据
    fun rawSeekFirst(): IndexNodeValue? {
        return primaryIndex.tree.seekFirst()
    }

    fun rawNextRecord(pos: IndexNodeValue?): IndexNodeValue? {
        if(pos==null) {
            return null
        }
        return primaryIndex.tree.nextRecordPosition(pos)
    }

    // raw find range by condition
    fun rawFind(condition: KeyCondition): List<IndexLeafNodeValue> {
        val first = primaryIndex.tree.seekByCondition(condition) ?: return listOf()
        val mutableResult = mutableListOf<IndexLeafNodeValue>()
        var cur: IndexNodeValue? = first
        while(cur!=null) {
            val record = cur.leafRecord()
            if(condition.match(record.key)) {
                mutableResult += record
                cur = primaryIndex.tree.nextRecordPosition(cur)
            } else {
                break
            }
        }
        return mutableResult.toList()
    }

    fun validate(): Boolean {
        return primaryIndex.tree.validate()
    }

    fun toFullTreeString(): String {
        return primaryIndex.tree.toFullTreeString()
    }

    override fun toString(): String {
        return "table $tblName (${columns.joinToString(", ") })" + (if(secondaryIndexes.isNotEmpty()) "\n\t${secondaryIndexes.joinToString("\n\t")}" else "")
    }
}