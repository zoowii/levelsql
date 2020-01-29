package com.zoowii.levelsql.engine

import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.store.IStore
import com.zoowii.levelsql.engine.store.LevelDbStore

class Database(val dbName: String, val store: IStore) {
    private var tables: List<Table> = listOf()

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