package com.zoowii.levelsql.engine.planner.source

import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.TableColumnDefinition

interface ISqlTableSource {
    fun seekFirst(sess: IDbSession): RowWithPosition?
    fun seekNextRecord(sess: IDbSession, currentPos: RowWithPosition): RowWithPosition?
    fun getColumns(): List<TableColumnDefinition>
}