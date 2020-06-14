package com.zoowii.levelsql.engine.planner.source

import com.zoowii.levelsql.engine.types.Row

data class RowWithPosition(val row: Row, val position: Any)
