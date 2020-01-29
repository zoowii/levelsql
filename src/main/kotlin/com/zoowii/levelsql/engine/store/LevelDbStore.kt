package com.zoowii.levelsql.engine.store

import org.iq80.leveldb.DB
import org.iq80.leveldb.Options
import org.iq80.leveldb.impl.Iq80DBFactory
import java.io.Closeable
import java.io.File

// 基于leveldb的store实现。因为leveldb本身就可以用seek来根据条件select，所以只用于测试用，意义不大
class LevelDbStore(private val db: DB) : IStore {
    companion object {
        fun openStore(dirFile: File): IStore {
            try {
                val db = Iq80DBFactory.factory.open(File(dirFile, "db"), Options().createIfMissing(true))
                return LevelDbStore(db)
            } catch (e: Exception) {
                throw e
            }
        }
    }

    override fun put(key: StoreKey, value: ByteArray) {
        db.put(key.toBytes(), value)
    }

    override fun get(key: StoreKey): ByteArray? {
        return db.get(key.toBytes())
    }

    override fun close() {
        db.close()
    }
}

