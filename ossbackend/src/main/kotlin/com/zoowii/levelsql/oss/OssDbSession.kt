package com.zoowii.levelsql.oss


import com.mashape.unirest.http.Unirest
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.planner.source.ISqlEngineSource
import com.zoowii.levelsql.engine.utils.logger
import org.apache.http.HttpStatus
import java.io.Closeable
import java.io.IOException
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicLong

data class OssFileStream(val url: String, var offset: Long = 0) : Closeable {
    val ossStream: RandomAccessStream

    init {
        val res = Unirest.get(url).asBinary()
        if (res.status != HttpStatus.SC_OK) {
            throw IOException(url)
        }
        val stream = res.body
        ossStream = RandomAccessStream(stream)
    }

    override fun close() {
        ossStream.close()
    }
}


class OssDbSession(val databaseDefinition: OssDatabaseDefinition) : IDbSession {
    private val log = logger()

    companion object {
        private val idGen = AtomicLong()
    }

    override val id = idGen.getAndIncrement()

    private var sqlEngineSource: ISqlEngineSource = OssSqlEngineSource()

    val dbFiles = mutableMapOf<String, OssFileStream>() // filepath => CsvFileStream

    fun getOssFileOrOpen(url: String): OssFileStream {
        return dbFiles.getOrPut(url, {
            log.info("open oss url {}", url)
            OssFileStream(url)
        })
    }

    val ossUrlExistsCheckCache = mutableMapOf<String, Boolean>()
    fun checkExistsOssUrl(ossUrl: String): Boolean {
        return ossUrlExistsCheckCache.getOrPut(ossUrl, {
            val res = Unirest.get(ossUrl).asString()
            // TODO: use oss sdk to check file exists
            res.status == HttpStatus.SC_OK
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