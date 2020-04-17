package com.zoowii.levelsql.engine.tx

import com.zoowii.levelsql.engine.Table
import com.zoowii.levelsql.engine.store.RowId
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.Row

interface Transaction {
    /**
     * @throws TransactionException
     */
    fun begin()

    /**
     * @throws TransactionException
     */
    fun commit()

    /**
     * @throws TransactionException
     */
    fun rollback()

    fun addInsertRecord(dbName: String, table: Table, key: Datum, rowId: RowId, row: Row)

    fun addUpdateRecord(dbName: String, table: Table, rowId: RowId, oldRowValue: Row, newRowValue: Row)

    fun addDeleteRecord(dbName: String, table: Table, rowId: RowId, oldRowValue: Row)

    // 2PC加锁操作需要在添加undo日志后进行
    /**
     * 2阶段提交Percolator算法中给primaryRow加锁
     * @throws TransactionException
     */
    fun addLockIfPrimaryRowIn2PC(dbName: String, table: Table, rowId: RowId, row: Row)
    /**
     * 2阶段提交Percolator算法中给secondaryRows加锁
     * @throws TransactionException
     */
    fun addLockIfSecondaryRowIn2PC(dbName: String, table: Table, rowId: RowId, row: Row)
    /**
     * 2阶段提交Percolator算法中执行commitTs写入W列以及删除锁的操作
     * @throws TransactionException
     */
    fun doCommitWriteIn2PC(dbName: String)
    /**
     * 2阶段提交Percolator算法中执行回滚和删除锁的操作
     * @throws TransactionException
     */
    fun doRollbackIn2PC(dbName: String)
    /**
     * 2阶段提交Percolator算法中发现依赖的某行记录有超时的锁时可以移除这个锁(这个方法只修改row不提交row)
     * @throws TransactionException
     */
    fun cleanLockIn2PC(dbName: String, table: Table, rowId: RowId, row: Row)
}