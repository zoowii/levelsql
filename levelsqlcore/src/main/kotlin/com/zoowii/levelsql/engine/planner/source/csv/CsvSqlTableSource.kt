package com.zoowii.levelsql.engine.planner.source.csv

import com.zoowii.levelsql.engine.*
import com.zoowii.levelsql.engine.planner.source.ISqlTableSource
import com.zoowii.levelsql.engine.planner.source.RowWithPosition
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumType
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import java.sql.SQLException

class CsvSqlTableSource(val tableDefinition: CsvTableDefinition) : ISqlTableSource {

    override fun seekFirst(sess: IDbSession): RowWithPosition? {
        sess as CsvDbSession
        val csvFileStream = sess.getCsvFileOrOpen(tableDefinition.csvFilepath)
        val csvFile = csvFileStream.csvFile
        if (csvFileStream.offset != 0L) {
            csvFile.seek(0)
            csvFileStream.offset = 0
        }
        val fileLen = csvFile.length()
        if (csvFileStream.offset >= fileLen) {
            return null
        }
        val line = csvFile.readLine()
        if (line.isEmpty()) {
            return null
        }
        val row = lineToRow(line)
        return CsvRowWithPosition(row, csvFileStream.offset)
    }

    override fun seekNextRecord(sess: IDbSession, currentPos: RowWithPosition): RowWithPosition? {
        sess as CsvDbSession
        currentPos as CsvRowWithPosition
        val csvFileStream = sess.getCsvFileOrOpen(tableDefinition.csvFilepath)
        val csvFile = csvFileStream.csvFile
        if (csvFileStream.offset != currentPos.offset) {
            csvFile.seek(currentPos.offset)
            csvFileStream.offset = currentPos.offset
        }
        val fileLen = csvFile.length()
        if (csvFileStream.offset >= fileLen) {
            return null
        }
        val line = csvFile.readLine()
        if (line.isNullOrBlank()) {
            return null
        }
        val row = lineToRow(line)
        return CsvRowWithPosition(row, csvFileStream.offset)
    }

    private fun lineToRow(line: String): Row {
        val row = Row()
        val columns = tableDefinition.columns
        val splited = line.split(",")
        if (splited.size != columns.size) {
            throw SQLException("row $line columns count not match headers")
        }
        row.data = splited.mapIndexed { index, str ->
            val column = columns[index]
            when (column.columnType) {
                is TextColumnType -> Datum(DatumTypes.kindText, stringValue = str)
                is VarCharColumnType -> Datum(DatumTypes.kindString, stringValue = str)
                is IntColumnType -> Datum(DatumTypes.kindInt64, intValue = str.toLong())
                is BoolColumnType -> Datum(DatumTypes.kindBool, boolValue = str.toBoolean())
                else -> throw SQLException("not supported column type $column")
            }
        }
        return row
    }

    override fun getColumns(): List<TableColumnDefinition> {
        return tableDefinition.columns.map { TableColumnDefinition(it.name, it.columnType, true) }
    }
}