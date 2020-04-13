package com.zoowii.levelsql.engine.tx

import com.zoowii.levelsql.engine.store.RowId
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.logger
import java.util.concurrent.atomic.AtomicInteger

class UndoTransaction : Transaction {
    private val id: Long

    companion object {
        private val log = logger()
        private val txSeq = AtomicInteger(0) // 同一秒内的交易用txSeq来区分事务顺序和id
    }

    init {
        id = java.time.Instant.now().epochSecond + txSeq.getAndIncrement() // TODO: fetch last txid from db store
        // TODO: init undo log IStore for every database. each undo log item have tx id and startTs
    }

    override fun begin() {
        // TODO
        log.debug("tx begin")
    }

    override fun commit() {
        // TODO
        log.debug("tx commit")
    }

    override fun rollback() {
        // TODO
        log.debug("tx rollback")
    }

    override fun addInsertRecord(dbName: String, rowId: RowId) {
        // TODO
    }

    override fun addUpdateRecord(dbName: String, rowId: RowId, oldRowValue: Row) {
        // TODO
    }

    override fun addDeleteRecord(dbName: String, rowId: RowId, oldRowValue: Row) {
        // TODO
    }
}