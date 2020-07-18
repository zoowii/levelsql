package com.zoowii.levelsql.oss


import com.zoowii.levelsql.engine.ColumnType

data class OssColumnDefinition(val name: String, val columnType: ColumnType)

/**
 * tableName, fileSequence(start from 0), type(raw,index,meta,etc.)
 */
typealias OssTableFileFinderRule = (String, Int, OssFileType) -> String

enum class OssFileType {
    Raw,
    Index,
    Meta
}

data class OssDbBaseDefinition(val ossBaseUrl: String,
                               val tableNames: List<String>,
                               val tableFileRules: OssTableFileFinderRule,
                               val tableFileIgnoreHeader: Boolean)

data class OssTableDefinition(val tblName: String,
                              val columns: List<OssColumnDefinition>,
                              val ossBaseDefinition: OssDbBaseDefinition)

data class OssDatabaseDefinition(val dbName: String,
                                 val tables: List<OssTableDefinition>,
                                 val ossBaseDefinition: OssDbBaseDefinition)
