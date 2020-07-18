package com.zoowii.levelsql.oss


import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.planner.source.ISqlEngineSource
import com.zoowii.levelsql.engine.planner.source.ISqlTableSource

class OssSqlEngineSource : ISqlEngineSource {
    override fun openTable(sess: IDbSession, tblName: String): ISqlTableSource? {
        sess as OssDbSession
        // TODO: 如果已经打开，考虑是否复用
        val tblDef = sess.databaseDefinition.tables.firstOrNull { it.tblName == tblName } ?: return null
        return OssSqlTableSource(tblDef)
    }
}