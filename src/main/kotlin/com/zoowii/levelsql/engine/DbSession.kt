package com.zoowii.levelsql.engine

import java.util.concurrent.atomic.AtomicLong

// 一次db会话的上下文
class DbSession(val engine: LevelSqlEngine) {
    companion object {
        private val idGen = AtomicLong()
    }
    val id = idGen.getAndIncrement()
    var db: Database? = null // 会话当前使用的数据库

    fun useDb(dbName: String) {
        db = engine.openDatabase(dbName)
    }
}