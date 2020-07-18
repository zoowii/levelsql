package com.zoowii.levelsql.engine.planner.source.levelsql

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.Table
import com.zoowii.levelsql.engine.TableColumnDefinition
import com.zoowii.levelsql.engine.planner.source.ISqlTableSource
import com.zoowii.levelsql.engine.planner.source.RowWithPosition
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.ByteArrayStream

class LevelSqlTableSource(private val table: Table) : ISqlTableSource {
    override fun seekFirst(sess: IDbSession): RowWithPosition? {
        sess as DbSession
        val seekedPos = table.rawSeekFirst(sess) ?: return null
        val record = seekedPos.leafRecord()
        val row = Row().fromBytes(ByteArrayStream(record.value))
        return LevelSqlRowWithPosition(row, seekedPos)
    }

    override fun seekNextRecord(sess: IDbSession, currentPos: RowWithPosition): RowWithPosition? {
        sess as DbSession
        currentPos as LevelSqlRowWithPosition
        val posObj = currentPos.position
        val seekedPos = table.rawNextRecord(sess, posObj) ?: return null
        val record = seekedPos.leafRecord()
        val row = Row().fromBytes(ByteArrayStream(record.value))
        return LevelSqlRowWithPosition(row, seekedPos)
    }

    override fun getColumns(): List<TableColumnDefinition> {
        return table.columns
    }
}