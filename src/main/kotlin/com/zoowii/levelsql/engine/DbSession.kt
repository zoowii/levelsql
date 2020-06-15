package com.zoowii.levelsql.engine

import com.zoowii.levelsql.engine.planner.source.ISqlEngineSource
import com.zoowii.levelsql.engine.planner.source.levelsql.LevelSqlEngineSource
import com.zoowii.levelsql.engine.tx.*
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicLong

interface IDbSession {
    val id: Long
    fun containsDb(dbName: String): Boolean
    fun verifyDbOpened(): Boolean
    fun getSqlEngineSource(): ISqlEngineSource?
    fun useDb(dbName: String)
}

// 一次db会话的上下文
class DbSession(val engine: LevelSqlEngine) : IDbSession {
    companion object {
        private val idGen = AtomicLong()
    }
    override val id = idGen.getAndIncrement()
    var db: Database? = null // 会话当前使用的数据库
    private var tx: Transaction? = null

    private var sqlEngineSource: ISqlEngineSource? = LevelSqlEngineSource()

    override fun getSqlEngineSource(): ISqlEngineSource? {
        return sqlEngineSource
    }

    override fun containsDb(dbName: String): Boolean {
        return engine.containsDatabase(dbName)
    }

    override fun useDb(dbName: String) {
        db = engine.openDatabase(dbName)
    }

    /**
     * @throws TransactionException
     */
    fun beginTransaction(): Transaction {
        synchronized(this) {
            if(tx != null) {
                throw TransactionException("tx can't begin more than once in one session")
            }
            val newTx = UndoTransaction(engine)
            newTx.begin()
            tx = newTx
            return newTx
        }
    }

    /**
     * @throws TransactionException
     */
    fun commitTransaction() {
        val sessTx = tx ?: return
        sessTx.commit()
    }

    /**
     * @throws TransactionException
     */
    fun rollbackTransaction() {
        val sessTx = tx ?: return
        sessTx.rollback()
    }

    fun inTransaction(): Boolean {
        return tx != null
    }

    fun getTransaction(): Transaction? {
        return tx
    }

    override fun verifyDbOpened(): Boolean {
        return db != null
    }
}