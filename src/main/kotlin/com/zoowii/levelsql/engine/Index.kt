package com.zoowii.levelsql.engine

import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.index.IndexTree
import com.zoowii.levelsql.engine.store.toBytes
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import java.io.ByteArrayOutputStream

// 支持单字段索引和联合索引
// 比如3个列A,B,C构成的联合索引，查询时条件只有A和B，则把(A+B+和C对应的固定长度空bytes)合并成一个bytes作为key去树中检索数据
class Index(val table: Table, val indexName: String, val columns: List<String>, val unique: Boolean = false, val primary: Boolean=false) {
    val tree = IndexTree(table.db.store, "db_${table.db.dbName}_table_${table.tblName}_primary_index",
            table.nodeBytesSize, table.treeDegree, true)

    fun metaToBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(table.db.dbName.toBytes())
        out.write(table.tblName.toBytes())
        out.write(indexName.toBytes())
        out.write(columns.size.toBytes())
        columns.map {
            out.write(it.toBytes())
        }
        out.write(unique.toBytes())
        out.write(primary.toBytes())
        return out.toByteArray()
    }

    companion object {
        fun metaFromBytes(table: Table, stream: ByteArrayStream): Index {
            val dbName = stream.unpackString()
            if(table.db.dbName != dbName) {
                throw DbException("invalid database name when load index meta")
            }
            val tblName = stream.unpackString()
            if(table.tblName != tblName) {
                throw DbException("invalid table name when load index meta")
            }
            val indexName = stream.unpackString()
            val columnsCount = stream.unpackInt32()
            val columns = (0 until columnsCount).map {
                stream.unpackString()
            }
            val unique = stream.unpackBoolean()
            val primary = stream.unpackBoolean()
            return Index(table, indexName, columns, unique, primary)
        }
    }

    override fun toString(): String {
        return "index $indexName (${columns.joinToString(", ")}) on ${table.db.dbName}.${table.tblName}" +
                (if(primary) " primary key" else "") +
                (if(unique) " unique key" else "")
    }
}