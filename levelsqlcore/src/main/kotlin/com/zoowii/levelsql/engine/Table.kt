package com.zoowii.levelsql.engine

import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.exceptions.IndexException
import com.zoowii.levelsql.engine.index.IndexLeafNodeValue
import com.zoowii.levelsql.engine.index.IndexNodeValue
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
                val index = Index.metaFromBytes(table, stream)
                index.tree.initTree()
                index
            }
            return table
        }
    }

    // 聚集索引
    private val primaryIndex = Index(this, "${tblName}_primary_${primaryKey}", listOf(primaryKey), true, true)
    // 二级索引
    private var secondaryIndexes = listOf<Index>()

    private val rowIdKeyMapping = HashMap<Long, Datum>() // rowId => row key mapping

    private fun addRowIdKeyMapping(rowId: RowId, key: Datum) {
        // TODO: rowIdKeyMapping 要改成多层树状mapping，事务执行时在下面加一层子mapping，回滚时放弃子层，同一层同时执行的事务都结束时合并上去
        rowIdKeyMapping[rowId.longValue()] = key
    }

    fun findRowByRowId(session: DbSession?, rowId: RowId): Row? {
        val key = rowIdKeyMapping.getOrDefault(rowId.longValue(), null) ?: return null
        var nodePos = rawGet(session, key)
        while(nodePos!=null) {
            val record = nodePos.leafRecord()
            if(record.rowId == rowId) {
                return Row().fromBytes(ByteArrayStream(record.value))
            }
            nodePos = rawNextRecord(session, nodePos)
        }
        return null
    }

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
                .map { it.column }.toSet()
        val allIndexes = listOf(primaryIndex) + secondaryIndexes
        var maxMatchedColumnsCount = 0 // 能匹配到最多列的索引匹配到的列数
        var maxMatchIndex: Index? = null // 最佳匹配的索引
        for(index in allIndexes) {
            assert(index.columns.isNotEmpty())
            var matchedColumnsCount = 0
            for(col in index.columns) {
                // 联合索引需要最左匹配原则，从左开始依次匹配
                if(!maybeSelfTableColumns.contains(col)) {
                    break
                }
                matchedColumnsCount++
            }
            if(matchedColumnsCount > maxMatchedColumnsCount) {
                maxMatchedColumnsCount = matchedColumnsCount
                maxMatchIndex = index
            }
        }
        return maxMatchIndex
    }

    fun openIndex(session: DbSession?, indexName: String): Index? {
        if (primaryIndex.indexName == indexName) {
            return primaryIndex
        }
        return secondaryIndexes.firstOrNull { it.indexName == indexName }
    }

    fun openPrimaryIndex(): Index {
        return primaryIndex
    }

    fun createIndex(session: DbSession?, indexName: String, columns: List<String>, unique: Boolean): Index {
        val existed = openIndex(session, indexName)
        if (existed != null) {
            throw DbException("index name $tblName.$indexName conflict")
        }
        val index = Index(this, indexName, columns, unique, false)
        index.tree.initTree()
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

    // TODO: table中涉及到的rows需要记录rowId => primary key的映射到内存，方便快速根据rowId找到最新row数据

    // 把一行记录(按table的columns顺序提供的)中{selectColumns}各列的值筛选出来返回
    private fun mapRecordColumns(record: Row, selectColumns: List<String>): List<Datum> {
        val selectColumnsIndexes = selectColumns.map { selectCol ->
            columns.indexOfFirst { col -> col.name == selectCol }
        }
        val result = mutableListOf<Datum>()
        for(idx in selectColumnsIndexes) {
            if(idx < 0) {
                throw DbException("mapRecordColumns error of column name not existed in table")
            }
            if(idx >= record.data.size) {
                throw DbException("record row not is a full table record, column ${columns[idx].name} not found")
            }
            result.add(record.data[idx])
        }
        return result
    }

    fun rawInsert(session: DbSession?, key: Datum, record: Row, rowId: RowId?=null) {
        val recordRowId = rowId ?: nextRowId()
        session?.getTransaction()?.addInsertRecord(db.dbName, this, key, recordRowId, record)

        addRowIdKeyMapping(recordRowId, key)

        primaryIndex.tree.addKeyValue(IndexLeafNodeValue(recordRowId, datumsToIndexKey(key), record.toBytes()))
        // 二级索引也需要插入记录
        for(index in secondaryIndexes) {
            val indexKeyDatums = mapRecordColumns(record, index.columns)
            // TODO: index中能得到各列的类型，对于nullable值，indexKeyDatums生成IndexKey的过程需要根据列的类型固定长度
            val indexKey = datumsToIndexKey(indexKeyDatums)
            val itemRow = Row()
            itemRow.data = listOf(key) // 这里是存储主键的原值，当需要回表的时候在转成Datum后再转成IndexKey去做回表查询
            val value = itemRow.toBytes()
            index.tree.addKeyValue(IndexLeafNodeValue(recordRowId, indexKey, value))
        }

    }

    fun rawUpdate(session: DbSession?, rowId: RowId, key: Datum, record: Row) {
        val tx = session?.getTransaction()
        val oldValueLeafRecord = if(tx!=null) primaryIndex.tree.findLeafNodeByKeyAndRowId(datumsToIndexKey(key), rowId)?.leafRecord() else null
        val oldRow = if(oldValueLeafRecord!=null) Row().fromBytes(ByteArrayStream(oldValueLeafRecord.value)) else null

        addRowIdKeyMapping(rowId, key)

        tx?.addUpdateRecord(db.dbName, this, rowId, oldRow!!, record)

        try {
            primaryIndex.tree.replaceKeyValue(IndexLeafNodeValue(rowId, datumsToIndexKey(key), record.toBytes()))
        } catch (e: IndexException) {
            throw DbException(e.message!!)
        }
        // 二级索引也需要修改记录
        for(index in secondaryIndexes) {
            val indexKeyDatums = mapRecordColumns(record, index.columns)
            // TODO: index中能得到各列的类型，对于nullable值，indexKeyDatums生成IndexKey的过程需要根据列的类型固定长度
            val indexKey = datumsToIndexKey(indexKeyDatums)
            val itemRow = Row()
            itemRow.data = listOf(key) // 这里是存储主键的原值，当需要回表的时候在转成Datum后再转成IndexKey去做回表查询
            val value = itemRow.toBytes()
            index.tree.replaceKeyValue(IndexLeafNodeValue(rowId, indexKey, value))
        }
    }

    fun rawDelete(session: DbSession?, key: Datum, rowId: RowId) {
        val indexPrimaryKey = datumsToIndexKey(key)
        val nodeAndPos = primaryIndex.tree.findLeafNodeByKeyAndRowId(indexPrimaryKey, rowId)
        if (nodeAndPos == null) {
            throw DbException("record not found for delete")
        }
        addRowIdKeyMapping(rowId, key)
        // TODO: 改成标记row为deleted而不是物理删除
        val record = Row().fromBytes(ByteArrayStream(nodeAndPos.leafRecord().value))
        session?.getTransaction()?.addDeleteRecord(db.dbName, this, rowId, record)

        primaryIndex.tree.deleteByKeyAndRowId(indexPrimaryKey, rowId)
        // 二级索引也需要修改记录
        for(index in secondaryIndexes) {
            val indexKeyDatums = mapRecordColumns(record, index.columns)
            // TODO: index中能得到各列的类型，对于nullable值，indexKeyDatums生成IndexKey的过程需要根据列的类型固定长度
            val indexKey = datumsToIndexKey(indexKeyDatums)
            val itemRow = Row()
            itemRow.data = listOf(key) // 这里是存储主键的原值，当需要回表的时候在转成Datum后再转成IndexKey去做回表查询
            val value = itemRow.toBytes()
            index.tree.deleteByKeyAndRowId(indexKey, rowId)
        }
    }

    fun rawGet(session: DbSession?, key: Datum): IndexNodeValue? {
        val indexPrimaryKey = datumsToIndexKey(key)
        val (nodeAndPos, isNewNode) = primaryIndex.tree.findIndex(indexPrimaryKey, false)
        if (nodeAndPos == null || isNewNode) {
            return null
        }
        // 调用者在rawGet后要循环检查返回的值是否满足条件（因为有可能是裁减后的key满足条件)，如果不满足就找下一项(如果没结束的话)
        return nodeAndPos
    }

    // 找到表中第一条数据
    fun rawSeekFirst(session: DbSession?): IndexNodeValue? {
        return primaryIndex.tree.seekFirst()
    }

    fun rawNextRecord(session: DbSession?, pos: IndexNodeValue?): IndexNodeValue? {
        if (pos == null) {
            return null
        }
        return primaryIndex.tree.nextRecordPosition(pos)
    }

    // raw find range by condition
    fun rawFind(session: DbSession?, condition: KeyCondition): List<IndexLeafNodeValue> {
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