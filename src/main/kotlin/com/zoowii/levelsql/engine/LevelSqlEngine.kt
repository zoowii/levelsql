package com.zoowii.levelsql.engine

import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.store.*
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import java.io.ByteArrayOutputStream
import java.sql.SQLException

class LevelSqlEngine(val store: IStore) {
    private var databases: List<Database> = listOf()

    // 从store中加载元信息
    fun loadMeta() {
        val metaBytes = store.get(metaStoreKey())
                ?: throw SQLException("load engine error")
        metaFromBytes(this, metaBytes)
        for(db in databases) {
            db.loadMeta()
        }
    }

    fun saveMeta() {
        val metaBytes = metaToBytes()
        store.put(metaStoreKey(), metaBytes)
    }

    private fun metaStoreKey(): StoreKey {
        return StoreKey("engine_meta", -1)
    }

    fun metaToBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(databases.size.toBytes())
        for (db in databases) {
            out.write(db.dbName.toBytes())
        }
        return out.toByteArray()
    }

    companion object {
        fun metaFromBytes(engine: LevelSqlEngine, data: ByteArray): Pair<LevelSqlEngine, ByteArray> {
            val stream = ByteArrayStream(data)
            val dbsCount = stream.unpackInt32()
            val dbs = mutableListOf<Database>()
            for (i in 0 until dbsCount) {
                val dbName = stream.unpackString()
                dbs += Database(dbName, engine.store)
            }
            engine.databases = dbs
            return Pair(engine, stream.remaining)
        }
    }

    fun createDatabase(dbName: String): Database {
        if (databases.any { it.dbName == dbName }) {
            throw DbException("database ${dbName} existed before")
        }
        val db = Database(dbName, store)
        databases += db
        return db
    }

    fun openDatabase(dbName: String): Database {
        return databases.firstOrNull { it.dbName == dbName } ?: throw DbException("database ${dbName} not found")
    }

    override fun toString(): String {
        return "engine: \n${databases.map { "\t${it.dbName}" }.joinToString("\n")}"
    }

}