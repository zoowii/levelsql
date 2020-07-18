package com.zoowii.levelsql.oss


import com.zoowii.levelsql.engine.*
import com.zoowii.levelsql.engine.planner.source.ISqlTableSource
import com.zoowii.levelsql.engine.planner.source.RowWithPosition
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.logger
import java.sql.SQLException

class OssSqlTableSource(val tableDefinition: OssTableDefinition) : ISqlTableSource {
    private val log = logger()

    override fun seekFirst(sess: IDbSession): RowWithPosition? {
        sess as OssDbSession
        val baseDefinition = tableDefinition.ossBaseDefinition
        val tableFileFinderRule = baseDefinition.tableFileRules
        val seq = 0
        val fileName = tableFileFinderRule(tableDefinition.tblName, seq, OssFileType.Raw)
        val fileUrl = baseDefinition.ossBaseUrl + "/" + fileName
        if (!sess.checkExistsOssUrl(fileUrl)) {
            return null
        }
        val ossFileStream = sess.getOssFileOrOpen(fileUrl)
        val ossStream = ossFileStream.ossStream
        if (ossFileStream.offset != 0L) {
            ossStream.seek(0)
            ossFileStream.offset = 0
        }
        if (ossStream.eof(ossFileStream.offset)) {
            return null
        }
        if (baseDefinition.tableFileIgnoreHeader) {
            ossStream.readLine()
            ossFileStream.offset = ossStream.offset()
            if (ossStream.eof(ossFileStream.offset)) {
                return null
            }
        }
        val line = ossStream.readLine()
        if (line.isNullOrEmpty()) {
            return null
        }
        val row = lineToRow(line)
        return OssRowWithPosition(row, ossFileStream.offset, ossFileStream, seq)
    }

    override fun seekNextRecord(sess: IDbSession, currentPos: RowWithPosition): RowWithPosition? {
        sess as OssDbSession
        currentPos as OssRowWithPosition
        val baseDefinition = tableDefinition.ossBaseDefinition
        val tableFileFinderRule = baseDefinition.tableFileRules
        // 如果currentPos到了所在stream的末尾了，则seq增加1
        val seq = if (currentPos.stream.ossStream.eof(currentPos.offset))
            (currentPos.ossFileSeq + 1)
        else currentPos.ossFileSeq
        val fileName = tableFileFinderRule(tableDefinition.tblName, seq, OssFileType.Raw)
        val fileUrl = baseDefinition.ossBaseUrl + "/" + fileName
        if (!sess.checkExistsOssUrl(fileUrl)) {
            return null
        }
        val ossFileStream = sess.getOssFileOrOpen(fileUrl)
        val ossStream = ossFileStream.ossStream
        if (ossStream.eof(0)) {
            return null
        }
        if (ossFileStream.offset != currentPos.offset) {
            ossStream.seek(currentPos.offset)
            ossFileStream.offset = currentPos.offset
        }
        if (ossStream.offset() == 0L && baseDefinition.tableFileIgnoreHeader) {
            ossStream.readLine()
            ossFileStream.offset = ossStream.offset()
            if (ossStream.eof(ossFileStream.offset)) {
                return null
            }
        }
        if (ossStream.eof(ossFileStream.offset)) {
            return null
        }
        val line = ossStream.readLine()
        if (line.isNullOrBlank()) {
            return null
        }
        val row = lineToRow(line)
        return OssRowWithPosition(row, ossFileStream.offset, ossFileStream, seq)
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