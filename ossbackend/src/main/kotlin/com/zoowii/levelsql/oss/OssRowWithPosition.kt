package com.zoowii.levelsql.oss


import com.zoowii.levelsql.engine.planner.source.RowWithPosition
import com.zoowii.levelsql.engine.types.Row

class OssRowWithPosition(private val row: Row, val offset: Long, val stream: OssFileStream, val ossFileSeq: Int) : RowWithPosition {
    override fun getRow(): Row {
        return row
    }
}