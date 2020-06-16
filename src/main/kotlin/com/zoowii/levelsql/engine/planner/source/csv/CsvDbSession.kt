package com.zoowii.levelsql.engine.planner.source.csv

import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.planner.source.ISqlEngineSource
import java.io.File
import java.io.RandomAccessFile
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicLong

data class CsvFileStream(val filepath: String, var offset: Long = 0) {
    val csvFile = RandomAccessFile(filepath, "r")
}

class CsvDbSession(val databaseDefinition: CsvDatabaseDefinition) : IDbSession {
    companion object {
        private val idGen = AtomicLong()
    }

    override val id = idGen.getAndIncrement()

    private var sqlEngineSource: ISqlEngineSource = CsvSqlEngineSource()

    val dbFiles = mutableMapOf<String, CsvFileStream>() // filepath => CsvFileStream

    fun getCsvFileOrOpen(filepath: String): CsvFileStream {
        return dbFiles.getOrPut(filepath, {
            CsvFileStream(filepath)
        })
    }

    override fun containsDb(dbName: String): Boolean {
        return dbName == databaseDefinition.dbName
    }

    override fun verifyDbOpened(): Boolean {
        return true
    }

    override fun getSqlEngineSource(): ISqlEngineSource? {
        return sqlEngineSource
    }

    override fun useDb(toUseDbName: String) {
        if (databaseDefinition.dbName != toUseDbName) {
            throw SQLException("db name $toUseDbName not exists")
        }
    }
}