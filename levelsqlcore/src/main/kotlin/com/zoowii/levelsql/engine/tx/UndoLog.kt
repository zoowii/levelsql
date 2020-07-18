package com.zoowii.levelsql.engine.tx

import com.alibaba.fastjson.JSON
import com.zoowii.levelsql.engine.store.RowId
import com.zoowii.levelsql.engine.types.Datum

object UndoLogActions {
    const val INSERT = "insert"
    const val UPDATE = "update"
    const val DELETE = "delete"
    const val COMMIT = "commit"
}

data class UndoLogItem(val action: String, val dbName: String, val tableName: String?=null,
                       val key: Datum?=null,
                       val rowId: RowId?=null, val oldValue: ByteArray?=null, val txid: Long?=null) {
    fun toBytes(): ByteArray {
        val data = java.util.HashMap<String, Any?>()
        data["action"] = action
        data["dbName"] = dbName
        data["tableName"] = tableName
        data["key"] = key?.toBytes()
        data["rowId"] = rowId?.longValue()
        data["old"] = oldValue
        return JSON.toJSONBytes(data)
    }
}
