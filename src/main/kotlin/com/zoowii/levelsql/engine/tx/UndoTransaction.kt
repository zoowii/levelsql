package com.zoowii.levelsql.engine.tx

import com.alibaba.fastjson.JSON
import com.zoowii.levelsql.engine.LevelSqlEngine
import com.zoowii.levelsql.engine.Table
import com.zoowii.levelsql.engine.store.*
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import com.zoowii.levelsql.engine.utils.logger
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class UndoTransaction(val engine: LevelSqlEngine) : Transaction {
    private val id: Long
    private val startTs: Long = Instant.now().epochSecond // TODO: 改成从database获取一个全局增加的序列号
    private var primaryRowId: RowId? = null
    private val store = engine.store
    private var state = TransactionState.Active
    private val undoLogs = java.util.concurrent.ConcurrentLinkedQueue<UndoLogItem>()

    companion object {
        private val log = logger()
        private val txSeq = AtomicInteger(0) // 同一秒内的交易用txSeq来区分事务顺序和id

        // TODO: 加载各dbName的undo log lastSeq到内存缓存,用一个static map缓存映射
        private val dbLastUndoLogSeq = java.util.concurrent.ConcurrentHashMap<String, CompletableFuture<Long>>() // dbName => lastUndoLastSeq

        private fun getDbLastUndoLogStoreKey(dbName: String): StoreKey {
            return StoreKey("db_${dbName}_last_undo_log", 0)
        }

        private fun getDbLastUndoLogSeq(store: IStore, dbName: String): CompletableFuture<Long> {
            return dbLastUndoLogSeq.getOrPut(dbName, { ->
                // async get last undo log seq from store
                val future = CompletableFuture<Long>()
                Thread { ->
                    val lastUndoLogSeqStoreKey = getDbLastUndoLogStoreKey(dbName)
                    val data = store.get(lastUndoLogSeqStoreKey)
                    if (data != null) {
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
        return StoreKey("db_${dbName}_undo_log", seq)
    }

    override fun begin() {
        log.debug("tx begin")
        if (state != TransactionState.Active) {
            throw TransactionException("invalid transaction state ${state.name} to begin")
        }
        state = TransactionState.PartiallyCommitted
    }

    override fun commit() {
        log.debug("tx start commit")
        if (state != TransactionState.PartiallyCommitted) {
            throw TransactionException("invalid transaction state ${state.name} to commit")
        }
        // do commit the tx and add a commit log
        if (!undoLogs.isEmpty()) {
            val dbName = undoLogs.peek().dbName

            doCommitWriteIn2PC(dbName)

            val undoLog = UndoLogItem(UndoLogActions.COMMIT, dbName, txid = id)
            addUndoLog(undoLog)
        }
        state = TransactionState.Committed
        log.debug("tx commited")
    }

    private fun performUndoLogItem(undoLogItem: UndoLogItem) {
        val action = undoLogItem.action
        try {
            when (action) {
                UndoLogActions.INSERT -> {
                    // delete from clustered table and indexes
                    val db = engine.openDatabase(undoLogItem.dbName)
                    val table = db.openTable(undoLogItem.tableName!!)
                    // TODO: 改成调用物理删除的方法
                    table.rawDelete(null, undoLogItem.key!!, undoLogItem.rowId!!)
                }
                UndoLogActions.UPDATE -> {
                    // rollback values in clustered table and indexes
                    val db = engine.openDatabase(undoLogItem.dbName)
                    val table = db.openTable(undoLogItem.tableName!!)
                    val oldRecord = Row().fromBytes(ByteArrayStream(undoLogItem.oldValue!!))
                    table.rawUpdate(null, undoLogItem.rowId!!, undoLogItem.key!!, oldRecord)
                }
                UndoLogActions.DELETE -> {
                    // TODO: 需要先把删除改成逻辑删除, 回滚时撤销这个标记
                    // add back row in clustered table and indexes
                    val db = engine.openDatabase(undoLogItem.dbName)
                    val table = db.openTable(undoLogItem.tableName!!)
                    val oldRecord = Row().fromBytes(ByteArrayStream(undoLogItem.oldValue!!))
                    table.rawInsert(null, undoLogItem.key!!, oldRecord, undoLogItem.rowId!!)
                }
                UndoLogActions.COMMIT -> {
                    throw TransactionException("committed tx can't be rollback")
                }
                else -> {
                    throw TransactionException("unknown undo action $action")
                }
            }
        } catch (e: Exception) {
            throw TransactionException(e.message!!)
        }
    }

    override fun rollback() {
        log.debug("tx start rollback")
        if (state != TransactionState.PartiallyCommitted) {
            throw TransactionException("invalid transaction state ${state.name} to rollback")
        }
        // do rollback of the tx by undo log
        while (!undoLogs.isEmpty()) {
            val item = undoLogs.poll()
            performUndoLogItem(item)
        }
        state = TransactionState.Failed
        log.debug("tx rollbacked")
    }

    private fun addUndoLog(undoLogItem: UndoLogItem) {
        undoLogs.add(undoLogItem)
        val lastUndoLogSeq = getDbLastUndoLogSeq(store, undoLogItem.dbName).get(5, TimeUnit.SECONDS)
        val undoLogBytes = undoLogItem.toBytes()
        val storeKey = undoLogStoreKey(undoLogItem.dbName, lastUndoLogSeq + 1)
        store.put(storeKey, undoLogBytes)
    }

    override fun addInsertRecord(dbName: String, table: Table, key: Datum, rowId: RowId, row: Row) {
        val undoLog = UndoLogItem(UndoLogActions.INSERT, dbName, tableName = table.tblName, key = key, rowId = rowId)
        addUndoLog(undoLog)
        addLockIfPrimaryRowIn2PC(dbName, table, rowId, row)
        addLockIfSecondaryRowIn2PC(dbName, table, rowId, row)
    }

    override fun addUpdateRecord(dbName: String, table: Table, rowId: RowId, oldRowValue: Row, newRowValue: Row) {
        val undoLog = UndoLogItem(UndoLogActions.UPDATE, dbName, tableName = table.tblName, rowId = rowId,
                oldValue = oldRowValue.toBytes())
        addUndoLog(undoLog)
        addLockIfPrimaryRowIn2PC(dbName, table, rowId, newRowValue)
        addLockIfSecondaryRowIn2PC(dbName, table, rowId, newRowValue)
    }

    override fun addDeleteRecord(dbName: String, table: Table, rowId: RowId, oldRowValue: Row) {
        val undoLog = UndoLogItem(UndoLogActions.DELETE, dbName, tableName = table.tblName, rowId = rowId,
                oldValue = oldRowValue.toBytes())
        addUndoLog(undoLog)
        addLockIfPrimaryRowIn2PC(dbName, table, rowId, oldRowValue)
        addLockIfSecondaryRowIn2PC(dbName, table, rowId, oldRowValue)
    }

    // 获取或者初始化row中的L列和W列（用于2阶段提交事务），返回L列和W列的索引
    private fun getOrSetLWColumnsInRowFor2PC(dbName: String, table: Table, rowId: RowId, row: Row): Pair<Int, Int> {
        if(row.data.size<=table.columns.size) {
            row.data = row.data + Datum(DatumTypes.kindNull) + Datum(DatumTypes.kindNull)
        }
        return Pair(table.columns.size, table.columns.size+1)
    }

    // prewrite阶段先给primaryRow的L列加锁以及检查是否有其他事务写入(检查W列的commitTs)等
    override fun addLockIfPrimaryRowIn2PC(dbName: String, table: Table, rowId: RowId, row: Row) {
        // 把第一条记录修改操作作为primary row
        if(undoLogs.size>1) {
            return
        }
        val (lIndex, wIndex) = getOrSetLWColumnsInRowFor2PC(dbName, table, rowId, row)
        val lCol = row.data[lIndex]
        if(lCol.kind!=DatumTypes.kindNull) {
            throw TransactionException("transaction lock conflict")
        }
        // 检查W列看是否有>=startTs的commitTs
        val wCol = row.data[wIndex]
        if(wCol.kind!=DatumTypes.kindNull) {
            val wContentStr = wCol.stringValue!!
            val wContent = JSON.parseArray(wContentStr)!!
            val rowCommitTs = wContent.getLong(1)
            if(rowCommitTs>=startTs) {
                throw TransactionException("transaction lock conflict, some row changed by other tx")
            }
        }
        // TODO: 加锁时提交给存储层时存储层需要判断锁，否则失败
        val lockContent = java.util.ArrayList<Any?>()
        lockContent += this.id
        lockContent += this.startTs
        val lockContentStr = JSON.toJSONString(lockContent)
        row.setItemByIndex(lIndex, Datum(DatumTypes.kindString, stringValue = lockContentStr))

        this.primaryRowId = rowId
    }

    // prewrite阶段primaryRow加锁后给其他的secondary rows加锁
    override fun addLockIfSecondaryRowIn2PC(dbName: String, table: Table, rowId: RowId, row: Row) {
        // 把第一条记录修改操作作为primary row，其他的为secondary rows
        if(undoLogs.size<=1) {
            return
        }
        val (lIndex, wIndex) = getOrSetLWColumnsInRowFor2PC(dbName, table, rowId, row)
        val lCol = row.data[lIndex]
        if(lCol.kind!=DatumTypes.kindNull) {
            if(lCol.kind != DatumTypes.kindString) {
                throw TransactionException("invalid L lock column type")
            }
            val lockContentStr = lCol.stringValue!!
            val lockContent = JSON.parseArray(lockContentStr)!!
            if(lockContent.getLong(0) != this.id) {
                throw TransactionException("tx ${this.id} visiting row locked by other txs")
            }
        }
        // 检查W列看是否有>=startTs的commitTs
        val wCol = row.data[wIndex]
        if(wCol.kind!=DatumTypes.kindNull) {
            val wContentStr = wCol.stringValue!!
            val wContent = JSON.parseArray(wContentStr)!!
            val rowCommitTs = wContent.getLong(1)
            if(rowCommitTs>=startTs) {
                throw TransactionException("transaction lock conflict, some row changed by other tx")
            }
        }
        // TODO: 加锁时提交给存储层时存储层需要判断锁，否则失败
        val lockContent = java.util.ArrayList<Any?>()
        lockContent += this.id
        lockContent += this.startTs
        lockContent += primaryRowId!!.longValue()
        val lockContentStr = JSON.toJSONString(lockContent)
        row.setItemByIndex(lIndex, Datum(DatumTypes.kindString, stringValue = lockContentStr))
    }

    private fun getCommitTs(): Long {
        return Instant.now().epochSecond // TODO: 从database获取递增的序列号
    }

    // prewrite阶段完成后的commit阶段，需要检查锁和修改W列
    override fun doCommitWriteIn2PC(dbName: String) {
        val commitTs = getCommitTs()
        // 先原子性修改primaryRow的W列，如果CAS修改W列失败（如果W列commitTs>当前事务commitTs则失败），则整个事务失败
        // primaryRow的W列成功后，因为其他secondary rows都记录了primaryRowId，所以已经可以知道事务提交成功了，可以异步的慢慢去修改W列
        // 也就是把prewrite阶段后的commit阶段变为一个primaryRow的W列的CAS原子操作
        val primaryRowId = this.primaryRowId ?: return
        // TODO: 从存储层查询primaryRow的最新数据并检查W列，然后CAS修改primaryRow列
    }

    override fun doRollbackIn2PC(dbName: String) {
        TODO("Not yet implemented")
    }

    override fun cleanLockIn2PC(dbName: String, table: Table, rowId: RowId, row: Row) {
        TODO("Not yet implemented")
    }
}