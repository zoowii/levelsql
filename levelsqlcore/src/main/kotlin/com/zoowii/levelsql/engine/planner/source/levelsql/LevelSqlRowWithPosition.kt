package com.zoowii.levelsql.engine.planner.source.levelsql

import com.zoowii.levelsql.engine.index.IndexNodeValue
import com.zoowii.levelsql.engine.planner.source.RowWithPosition
import com.zoowii.levelsql.engine.types.Row

class LevelSqlRowWithPosition(private val row: Row, val position: IndexNodeValue) : RowWithPosition {
    override fun getRow(): Row {
        return row
    }
}