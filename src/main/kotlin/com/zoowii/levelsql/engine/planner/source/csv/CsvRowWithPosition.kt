package com.zoowii.levelsql.engine.planner.source.csv

import com.zoowii.levelsql.engine.planner.source.RowWithPosition
import com.zoowii.levelsql.engine.types.Row

class CsvRowWithPosition(private val row: Row, val offset: Long) : RowWithPosition {
    override fun getRow(): Row {
        return row
    }
}