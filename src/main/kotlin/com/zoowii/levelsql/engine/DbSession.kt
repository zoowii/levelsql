package com.zoowii.levelsql.engine

import com.zoowii.levelsql.engine.tx.*
import java.util.concurrent.atomic.AtomicLong

// 一次db会话的上下文
class DbSession(val engine: LevelSqlEngine) {
    companion object {
        private val idGen = AtomicLong()
    }
    val id = idGen.getAndIncrement()
    var db: Database? = null // 会话当前使用的数据库
    private var tx: Transaction? = null

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
            val newTx = UndoTransaction()
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