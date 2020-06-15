package com.zoowii.levelsql.engine.planner.source.csv

import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.planner.source.ISqlEngineSource
import com.zoowii.levelsql.engine.planner.source.ISqlTableSource

class CsvSqlEngineSource : ISqlEngineSource {
    override fun openTable(sess: IDbSession, tblName: String): ISqlTableSource? {
        sess as CsvDbSession
        return CsvSqlTableSource(tblName, sess.headers)
    }
}