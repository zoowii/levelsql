package com.zoowii.levelsql.protocol.mysql

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.annotation.JSONField

typealias CommandType = Byte

abstract class MysqlPacket {
    companion object {
        // command packet types
        val CMD_SLEEP: CommandType = 0
        val CMD_QUIT: CommandType = 1
        val CMD_INIT_DB: CommandType = 2
        val CMD_QUERY: CommandType = 3
        val CMD_FIELD_LIST: CommandType = 4
        val CMD_CREATE_DB: CommandType = 5
        val CMD_DROP_DB: CommandType = 6
        val CMD_REFRESH: CommandType = 7
        val CMD_SHUTDOWN: CommandType = 8
        val CMD_STATISTICS: CommandType = 9
        val CMD_PROCESS_INFO: CommandType = 10
        val CMD_CONNECT: CommandType = 11
        val CMD_PROCESS_KILL: CommandType = 12
        val CMD_DEBUG: CommandType = 13
        val CMD_PING: CommandType = 14
        val CMD_TIME: CommandType = 15
        val CMD_DELAYED_INSERT: CommandType = 16
        val CMD_CHANGE_USER: CommandType = 17
        val CMD_BINLOG_DUMP: CommandType = 18
        val CMD_TABLE_DUMP: CommandType = 19
        val CMD_CONNECT_OUT: CommandType = 20
        val CMD_REGISTER_SLAVE: CommandType = 21
        val CMD_STMT_PREPARE: CommandType = 22
        val CMD_STMT_EXECUTE: CommandType = 23
        val CMD_STMT_SEND_LONG_DATA: CommandType = 24
        val CMD_STMT_CLOSE: CommandType = 25
        val CMD_STMT_RESET: CommandType = 26
        val CMD_SET_OPTION: CommandType = 27
        val CMD_STMT_FETCH: CommandType = 28
    }

    var packetLength: Int = 0
    var sequenceId: Byte = 0
    @JSONField(serialize = false, deserialize = false)
    var context: Context? = null

    abstract fun read(reader: ProtoStreamReader)
    abstract fun write(writer: ProtoStreamWriter)

    override fun toString(): String {
        return JSON.toJSONString(this)
    }
}