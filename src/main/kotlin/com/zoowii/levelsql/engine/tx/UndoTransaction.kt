package com.zoowii.levelsql.engine.tx

import com.alibaba.fastjson.JSON
import com.zoowii.levelsql.engine.LevelSqlEngine
import com.zoowii.levelsql.engine.store.*
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.logger
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class UndoTransaction(val engine: LevelSqlEngine) : Transaction {
    private val id: Long
    private val store = engine.store
    private var state = TransactionState.Active
    private val undoLogs = java.util.concurrent.ConcurrentLinkedQueue<UndoLogItem>()

    companion object {
        private val log = logger()
        private val txSeq = AtomicInteger(0) // 同一秒内的交易用txSeq来区分事务顺序和id
        // TODO: 加载各dbName的undo log lastSeq到内存缓存,用一个static map缓存映射
        private val dbLastUndoLogSeq = java.util.concurrent.ConcurrentHashMap<String, CompletableFuture<Long>>() // dbName => lastUndoLastSeq

        private fun getDbLastUndoLogStoreKey(dbName: String): StoreKey {
            return StoreKey("db:$dbName:last_undo_log", 0)
        }

        private fun getDbLastUndoLogSeq(store: IStore, dbName: String): CompletableFuture<Long> {
            return dbLastUndoLogSeq.getOrPut(dbName, { ->
                // async get last undo log seq from store
                val future = CompletableFuture<Long>()
                Thread {->
                    val lastUndoLogSeqStoreKey = getDbLastUndoLogStoreKey(dbName)
                    val data = store.get(lastUndoLogSeqStoreKey)
                    if(data != null) {
                        val seq = Int32FromBytes(data)
                        future.complete(seq.first.toLong())
                    } else {
                        future.complete(0L)
                    }
                }.start()
                return future
            })
        }
        private fun setDbLastUndoLogSeq(store: IStore, dbName: String, seq: Long) {
            dbLastUndoLogSeq[dbName] = CompletableFuture.completedFuture(seq)
            // async set last undo log seq to store
            val lastUndoLogSeqStoreKey = getDbLastUndoLogStoreKey(dbName)
            val data = seq.toInt().toBytes()
            store.put(lastUndoLogSeqStoreKey, data)
        }
    }

    init {
        id = java.time.Instant.now().epochSecond + txSeq.getAndIncrement() // TODO: fetch last txid from db store
    }

    private fun undoLogStoreKey(dbName: String, seq: Long): StoreKey {
        return StoreKey("db:$dbName:undo_log", seq)
    }

    override fun begin() {
        log.debug("tx begin")
        if(state!=TransactionState.Active) {
            throw TransactionException("invalid transaction state ${state.name} to begin")
        }
        state = TransactionState.PartiallyCommitted
    }

    override fun commit() {
        log.debug("tx commit")
        if(state != TransactionState.PartiallyCommitted) {
            throw TransactionException("invalid transaction state ${state.name} to commit")
        }
        // do commit the tx and add a commit log
        if(!undoLogs.isEmpty()) {
            val dbName = undoLogs.peek().dbName
            val undoLog = UndoLogItem(UndoLogActions.COMMIT, dbName, txid = id)
            addUndoLog(undoLog)
        }
        state = TransactionState.Committed
    }

    private fun performUndoLogItem(undoLogItem: UndoLogItem) {
        val action = undoLogItem.action
        when(action) {
            UndoLogActions.INSERT -> {
                // TODO: delete from clustered table and indexes
            }
            UndoLogActions.UPDATE -> {
                // TODO: rollback values in clustered table and indexes
            }
            UndoLogActions.DELETE -> {
                // TODO: add back row in clustered table and indexes
                // TODO: 需要先把删除改成逻辑删除, 回滚时撤销这个标记
            }
            UndoLogActions.COMMIT -> {
                throw TransactionException("committed tx can't be rollback")
            }
            else -> {
                throw TransactionException("unknown undo action $action")
            }
        }
    }

    override fun rollback() {
        log.debug("tx rollback")
        if(state != TransactionState.PartiallyCommitted) {
            throw TransactionException("invalid transaction state ${state.name} to rollback")
        }
        // TODO: do rollback of the tx by undo log
        while(!undoLogs.isEmpty()) {
            val item = undoLogs.poll()
            performUndoLogItem(item)
        }
        state = TransactionState.Failed
    }

    private fun addUndoLog(undoLogItem: UndoLogItem) {
        undoLogs.add(undoLogItem)
        val lastUndoLogSeq = getDbLastUndoLogSeq(store, undoLogItem.dbName).get(5, TimeUnit.SECONDS)
        val undoLogBytes = undoLogItem.toBytes()
        val storeKey = undoLogStoreKey(undoLogItem.dbName, lastUndoLogSeq+1)
        store.put(storeKey, undoLogBytes)
    }

    override fun addInsertRecord(dbName: String, tableName: String, rowId: RowId) {
        val undoLog = UndoLogItem(UndoLogActions.INSERT, dbName, tableName=tableName, rowId = rowId.longValue())
        addUndoLog(undoLog)
    }

    override fun addUpdateRecord(dbName: String, tableName: String, rowId: RowId, oldRowValue: Row) {
        val undoLog = UndoLogItem(UndoLogActions.UPDATE, dbName, tableName=tableName, rowId = rowId.longValue(), oldValue = oldRowValue.toBytes())
        addUndoLog(undoLog)
    }

    override fun addDeleteRecord(dbName: String, tableName: String, rowId: RowId, oldRowValue: Row) {
        val undoLog = UndoLogItem(UndoLogActions.DELETE, dbName, tableName=tableName, rowId = rowId.longValue(), oldValue = oldRowValue.toBytes())
        addUndoLog(undoLog)
    }
}