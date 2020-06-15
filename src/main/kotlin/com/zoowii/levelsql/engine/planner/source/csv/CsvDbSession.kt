package com.zoowii.levelsql.engine.planner.source.csv

import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.planner.source.ISqlEngineSource
import java.io.File
import java.io.RandomAccessFile
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicLong

// TODO: csv headers中设置各列的类型
class CsvDbSession(val dbName: String, val tblName: String, val headers: List<String>, val csvFilepath: String) : IDbSession {
    companion object {
        private val idGen = AtomicLong()
    }
    override val id = idGen.getAndIncrement()

    private var sqlEngineSource: ISqlEngineSource = CsvSqlEngineSource()

    val csvFile = RandomAccessFile(csvFilepath, "r")
    var offset: Long = 0

    override fun containsDb(dbName: String): Boolean {
        return dbName == dbName
    }

    override fun verifyDbOpened(): Boolean {
        return File(csvFilepath).exists()
    }

    override fun getSqlEngineSource(): ISqlEngineSource? {
        return sqlEngineSource
    }

    override fun useDb(toUseDbName: String) {
        if(dbName != toUseDbName) {
            throw SQLException("db name $toUseDbName not exists")
        }
    }
}