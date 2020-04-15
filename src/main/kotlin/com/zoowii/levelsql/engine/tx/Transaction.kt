package com.zoowii.levelsql.engine.tx

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

    fun addInsertRecord(dbName: String, tableName: String, key: Datum, rowId: RowId)

    fun addUpdateRecord(dbName: String, tableName: String, rowId: RowId, oldRowValue: Row)

    fun addDeleteRecord(dbName: String, tableName: String, rowId: RowId, oldRowValue: Row)
}