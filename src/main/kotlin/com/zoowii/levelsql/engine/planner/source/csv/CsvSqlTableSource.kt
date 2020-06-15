package com.zoowii.levelsql.engine.planner.source.csv

import com.zoowii.levelsql.engine.ColumnType
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.TableColumnDefinition
import com.zoowii.levelsql.engine.TextColumnType
import com.zoowii.levelsql.engine.planner.source.ISqlTableSource
import com.zoowii.levelsql.engine.planner.source.RowWithPosition
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumType
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import java.sql.SQLException

class CsvSqlTableSource(val tblName: String, val headers: List<String>) : ISqlTableSource {

    override fun seekFirst(sess: IDbSession): RowWithPosition? {
        sess as CsvDbSession
        val csvFile = sess.csvFile
        if(sess.offset != 0L) {
            csvFile.seek(0)
            sess.offset = 0
        }
        val fileLen = csvFile.length()
        if(sess.offset >= fileLen) {
            return null
        }
        val line = csvFile.readLine()
        if(line.isEmpty()) {
            return null
        }
        val row = Row()
        row.data = line.split(",").map { Datum(DatumTypes.kindString, stringValue = it) }
        if(row.data.size != headers.size) {
            throw SQLException("row $line columns count not match headers")
        }
        return CsvRowWithPosition(row, sess.offset)
    }

    override fun seekNextRecord(sess: IDbSession, currentPos: RowWithPosition): RowWithPosition? {
        sess as CsvDbSession
        currentPos as CsvRowWithPosition
        val csvFile = sess.csvFile
        if(sess.offset != currentPos.offset) {
            csvFile.seek(currentPos.offset)
            sess.offset = currentPos.offset
        }
        val fileLen = csvFile.length()
        if(sess.offset >= fileLen) {
            return null
        }
        val line = csvFile.readLine()
        if(line.isNullOrBlank()) {
            return null
        }
        val row = Row()
        row.data = line.split(",").map { Datum(DatumTypes.kindString, stringValue = it) }
        if(row.data.size != headers.size) {
            throw SQLException("row $line columns count not match headers")
        }
        return CsvRowWithPosition(row, sess.offset)
    }

    override fun getColumns(): List<TableColumnDefinition> {
        return headers.map { TableColumnDefinition(it, TextColumnType(), true) }
    }
}