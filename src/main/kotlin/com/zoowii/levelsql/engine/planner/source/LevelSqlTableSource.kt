package com.zoowii.levelsql.engine.planner.source

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.Table
import com.zoowii.levelsql.engine.TableColumnDefinition
import com.zoowii.levelsql.engine.index.IndexNodeValue
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.ByteArrayStream

class LevelSqlTableSource(private val table: Table) : ISqlTableSource {
    override fun seekFirst(sess: IDbSession): RowWithPosition? {
        sess as DbSession
        val seekedPos = table.rawSeekFirst(sess) ?: return null
        val record = seekedPos.leafRecord()
        val row = Row().fromBytes(ByteArrayStream(record.value))
        return RowWithPosition(row, seekedPos)
    }

    override fun seekNextRecord(sess: IDbSession, currentPos: RowWithPosition): RowWithPosition? {
        sess as DbSession
        val posObj = currentPos.position
        posObj as IndexNodeValue
        val seekedPos = table.rawNextRecord(sess, posObj) ?: return null
        val record = seekedPos.leafRecord()
        val row = Row().fromBytes(ByteArrayStream(record.value))
        return RowWithPosition(row, seekedPos)
    }

    override fun getColumns(): List<TableColumnDefinition> {
        return table.columns
    }
}