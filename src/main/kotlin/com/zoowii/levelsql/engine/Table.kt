package com.zoowii.levelsql.engine

import com.zoowii.levelsql.TableColumnDefinition
import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.exceptions.IndexException
import com.zoowii.levelsql.engine.index.IndexLeafNodeValue
import com.zoowii.levelsql.engine.index.IndexNodeValue
import com.zoowii.levelsql.engine.index.IndexTree
import com.zoowii.levelsql.engine.index.datumsToIndexKey
import com.zoowii.levelsql.engine.store.RowId
import com.zoowii.levelsql.engine.store.toBytes
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import com.zoowii.levelsql.engine.utils.KeyCondition
import com.zoowii.levelsql.sql.ast.ColumnHintInfo
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.concurrent.atomic.AtomicLong

class Table(val db: Database, val tblName: String, val primaryKey: String, val columns: List<TableColumnDefinition>,
            val nodeBytesSize: Int = 1024 * 16, val treeDegree: Int = 100) {

    fun metaToBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(db.dbName.toBytes())
        out.write(tblName.toBytes())
        out.write(primaryKey.toBytes())
        out.write(columns.size.toBytes())
        for (c in columns) {
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
            if (dbName != db.dbName) {
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

    fun containsIndex(indexName: String): Boolean {
        if (primaryIndex.indexName == indexName) {
            return true
        }
        return secondaryIndexes.any { it.indexName == indexName }
    }

    // 根据使用的列(table.column的列表)找到满足条件的索引
    fun findIndexByColumns(columnHints: List<ColumnHintInfo>): Index? {
        if(columnHints.isEmpty())
            return null
        // 找到使用的列中适用于本表的列
        val maybeSelfTableColumns = columnHints.filter { it.tblName==null || it.tblName == tblName }
                .map { it.column }
        val allIndexes = listOf(primaryIndex) + secondaryIndexes
        for(index in allIndexes) {
            assert(index.columns.isNotEmpty())
            val indexFirstColumn = index.columns[0] // 目前只使用索引的第一列
            if(maybeSelfTableColumns.contains(indexFirstColumn)) {
                return index
            }
        }
        return null
    }

    fun openIndex(indexName: String): Index? {
        if (primaryIndex.indexName == indexName) {
            return primaryIndex
        }
        return secondaryIndexes.firstOrNull { it.indexName == indexName }
    }

    fun createIndex(indexName: String, columns: List<String>, unique: Boolean): Index {
        val existed = openIndex(indexName)
        if (existed != null) {
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


    // TODO: raw方法改成主键key和record都用Datum以及Row的方式
    fun rawInsert(key: Datum, record: Row) {
        val nextRowId = nextRowId()
        primaryIndex.tree.addKeyValue(IndexLeafNodeValue(nextRowId, datumsToIndexKey(key), record.toBytes()))
        // TODO: 二级索引也需要插入记录
    }

    fun rawUpdate(rowId: RowId, key: Datum, record: Row) {
        try {
            primaryIndex.tree.replaceKeyValue(IndexLeafNodeValue(rowId, datumsToIndexKey(key), record.toBytes()))
        } catch (e: IndexException) {
            throw DbException(e.message!!)
        }
        // TODO: 二级索引也需要修改记录
    }

    fun rawDelete(key: Datum, rowId: RowId) {
        val indexPrimaryKey = datumsToIndexKey(key)
        val nodeAndPos = primaryIndex.tree.findLeafNodeByKeyAndRowId(indexPrimaryKey, rowId)
        if (nodeAndPos == null) {
            throw DbException("record not found for delete")
        }
        primaryIndex.tree.deleteByKeyAndRowId(indexPrimaryKey, rowId)
        // TODO: 二级索引也需要修改记录
    }

    fun rawGet(key: Datum): IndexLeafNodeValue? {
        val indexPrimaryKey = datumsToIndexKey(key)
        val (nodeAndPos, isNewNode) = primaryIndex.tree.findIndex(indexPrimaryKey, false)
        if (nodeAndPos == null || isNewNode) {
            return null
        }
        // 调用者在rawGet后要循环检查返回的值是否满足条件（因为有可能是裁减后的key满足条件)，如果不满足就找下一项(如果没结束的话)
        return nodeAndPos.node.values[nodeAndPos.indexInNode]
    }

    // 找到表中第一条数据
    fun rawSeekFirst(): IndexNodeValue? {
        return primaryIndex.tree.seekFirst()
    }

    fun rawNextRecord(pos: IndexNodeValue?): IndexNodeValue? {
        if (pos == null) {
            return null
        }
        return primaryIndex.tree.nextRecordPosition(pos)
    }

    // raw find range by condition
    fun rawFind(condition: KeyCondition): List<IndexLeafNodeValue> {
        val first = primaryIndex.tree.seekByCondition(condition) ?: return listOf()
        val mutableResult = mutableListOf<IndexLeafNodeValue>()
        var cur: IndexNodeValue? = first
        while (cur != null) {
            val record = cur.leafRecord()
            // TODO: condition内目前是包含了补齐裁减后的key的条件，不是原始key，需要另外保留原始key，record中也要保留原始key，方便检查是否完全满足
            if (condition.match(record.key)) {
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
        return "table $tblName (${columns.joinToString(", ")})" + (if (secondaryIndexes.isNotEmpty()) "\n\t${secondaryIndexes.joinToString("\n\t")}" else "")
    }
}