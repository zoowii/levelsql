package com.zoowii.levelsql.engine.planner.source.levelsql

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.planner.source.ISqlEngineSource
import com.zoowii.levelsql.engine.planner.source.ISqlTableSource

class LevelSqlEngineSource : ISqlEngineSource {

    override fun openTable(sess: IDbSession, tblName: String): ISqlTableSource? {
        sess as DbSession
        val db = sess.db!!
        val table = db.openTable(tblName)
        return LevelSqlTableSource(table)
    }
}
