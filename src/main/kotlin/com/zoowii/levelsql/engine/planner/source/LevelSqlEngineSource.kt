package com.zoowii.levelsql.engine.planner.source

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.IDbSession

class LevelSqlEngineSource : ISqlEngineSource {
    override fun openTable(sess: IDbSession, tblName: String): ISqlTableSource? {
        sess as DbSession
        val table = sess.db?.openTable(tblName) ?: return null
        return LevelSqlTableSource(table)
    }
}
