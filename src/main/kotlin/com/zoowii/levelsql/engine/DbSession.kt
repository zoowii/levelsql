package com.zoowii.levelsql.engine

import com.zoowii.levelsql.engine.planner.source.ISqlEngineSource
import com.zoowii.levelsql.engine.planner.source.LevelSqlEngineSource
import com.zoowii.levelsql.engine.tx.*
import java.util.concurrent.atomic.AtomicLong

interface IDbSession {}

// 一次db会话的上下文
class DbSession(val engine: LevelSqlEngine) : IDbSession {
    companion object {
        private val idGen = AtomicLong()
    }
    val id = idGen.getAndIncrement()
    var db: Database? = null // 会话当前使用的数据库
    private var tx: Transaction? = null

    var sqlEngineSource: ISqlEngineSource? = LevelSqlEngineSource()

    fun useDb(dbName: String) {
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
}