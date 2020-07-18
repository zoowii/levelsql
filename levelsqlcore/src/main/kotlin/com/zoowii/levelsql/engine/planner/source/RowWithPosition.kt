package com.zoowii.levelsql.engine.planner.source

import com.zoowii.levelsql.engine.types.Row

interface RowWithPosition {
    fun getRow(): Row
}
