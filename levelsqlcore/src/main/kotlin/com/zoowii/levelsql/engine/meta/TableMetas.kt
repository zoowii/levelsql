package com.zoowii.levelsql.engine.meta

data class DatabaseMeta(val name: String)

data class TableMeta(val dbName: String, val name: String)

data class RowId(val tableName: String, val sequence: Long)

data class IndexMeta(val tableName: String, val indexName: String, val unique: Boolean,
                     val primaryKey: Boolean, val columns: List<String>)
