package com.zoowii.levelsql.engine.planner.source

import com.zoowii.levelsql.engine.IDbSession

/**
 * sql查询的数据源接口，给sql planner查询使用
 */
interface ISqlEngineSource {
    fun openTable(sess: IDbSession, tblName: String): ISqlTableSource?
}
