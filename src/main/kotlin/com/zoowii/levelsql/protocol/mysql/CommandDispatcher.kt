package com.zoowii.levelsql.protocol.mysql

import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.protocol.mysql.packet.ErrPacket
import com.zoowii.levelsql.protocol.mysql.packet.OkPacket
import com.zoowii.levelsql.protocol.mysql.packet.QueryPacket

object CommandDispatcher {
    private val log = logger()
    fun dispatchCommand(context: Context, packet: MysqlCommandType): MysqlPacket {
        when(packet.javaClass) {
            QueryPacket::class.java -> {
                packet as QueryPacket
                val querySql = String(packet.message)
                log.debug("query packet message $querySql")
                // TODO: 处理不同的SQL逻辑
                val ok = context.create(OkPacket::class.java, context.nextSeqId())
                ok.info = "query successfully"
                return ok
            }
            else -> {
                log.error("not implemented processor for command type ${packet.javaClass.canonicalName}")
                val err = context.create(ErrPacket::class.java, context.nextSeqId())
                err.errorMessage = "not implemented processor for command type ${packet.javaClass.canonicalName}"
                err.errorCode = 10001
                err.sqlState = "10001"
                err.sqlStateMarker = "10001"
                return err
            }
        }
    }
}