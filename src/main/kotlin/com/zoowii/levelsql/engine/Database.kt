package com.zoowii.levelsql.engine

import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.store.*
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.sql.SQLException

class Database(val dbName: String, val store: IStore) {
    private var tables: List<Table> = listOf()

    // 从store中加载数据库元信息
    fun loadMeta() {
        val dbTablesMetaBytes = store.get(dbMetaTablesStoreKey())
                ?: throw SQLException("load db $dbName error")
        metaFromBytes(this, dbTablesMetaBytes)
    }

    fun saveMeta() {
        val metaBytes = metaToBytes()
        store.put(dbMetaTablesStoreKey(), metaBytes)
    }

    private fun dbMetaTablesStoreKey(): StoreKey {
        return StoreKey("db_${dbName}_meta_tables", -1)
    }

    fun metaToBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(dbName.toBytes())
        out.write(tables.size.toBytes())
        for (t in tables) {
            out.write(t.metaToBytes())
        }
        return out.toByteArray()
    }

    companion object {
        fun metaFromBytes(db: Database, data: ByteArray): Pair<Database, ByteArray> {
            val (dbName, remaining1) = StringFromBytes(data)
            if (dbName != db.dbName) {
                throw IOException("conflict db name $dbName and ${db.dbName} when load database")
            }
            val (tablesCount, remaining2) = Int32FromBytes(remaining1)
            var remaining = remaining2
            val dbTables = mutableListOf<Table>()
            for (i in 0 until tablesCount) {
                val (table, tmpRemaining) = Table.metaFromBytes(db, remaining)
                remaining = tmpRemaining
                dbTables += table
            }
            db.tables = dbTables
            return Pair(db, remaining)
        }
    }

    fun createTable(tableName: String): Table {
        if (tables.any { it.tblName == tableName }) {
            throw DbException("table ${tableName} existed before")
        }
        val tbl = Table(this, tableName)
        this.tables += tbl
        return tbl
    }

    fun openTable(tableName: String): Table {
        return tables.firstOrNull { it.tblName == tableName } ?: throw DbException("table ${tableName} not found")
    }
}