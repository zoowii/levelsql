package com.zoowii.levelsql.engine.planner.source.csv

import com.zoowii.levelsql.engine.ColumnType

data class CsvColumnDefinition(val name: String, val columnType: ColumnType)

data class CsvTableDefinition(val tblName: String,
                              val columns: List<CsvColumnDefinition>,
                              val csvFilepath: String)

data class CsvDatabaseDefinition(val dbName: String, val tables: List<CsvTableDefinition>)
