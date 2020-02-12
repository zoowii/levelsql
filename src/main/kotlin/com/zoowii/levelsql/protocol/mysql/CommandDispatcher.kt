package com.zoowii.levelsql.protocol.mysql

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.protocol.mysql.packet.ErrPacket
import com.zoowii.levelsql.protocol.mysql.packet.OkPacket
import com.zoowii.levelsql.protocol.mysql.packet.QueryPacket
import com.zoowii.levelsql.protocol.mysql.packet.ResultSetRowPacket

class CommandDispatcher(private val server: MysqlServer) {
    private val log = logger()
    fun dispatchCommand(context: Context, packet: MysqlCommandType): MysqlPacket {
        when(packet.javaClass) {
            QueryPacket::class.java -> {
                packet as QueryPacket
                val querySql = String(packet.message)
                log.debug("query packet message $querySql")

                // 执行SQL
                val engine = server.getEngine()
                val sess = engine.createSession()
                if(context.currentDb != null) {
                    sess.useDb(context.currentDb!!)
                }
                val sqlResultSet = engine.executeSQL(sess, querySql)
//                val resultSetPacket = context.create(ResultSetRowPacket::class.java, context.nextSeqId())
//                resultSetPacket.columnCount = sqlResultSet.columns.size
//                // 把headers输出
//                val headerRow = Row()
//                headerRow.data = sqlResultSet.columns.map { Datum(DatumTypes.kindString, stringValue = it) }
//                resultSetPacket.columnValues.add(headerRow.toBytes()) // TODO: row要转换成mysql的行的bytes格式
//                // 把chunk输出
//                for(row in sqlResultSet.chunk.rows) {
//                    resultSetPacket.columnValues.add(row.toBytes()) // TODO: row要转换成mysql的行的bytes格式
//                }
//                return resultSetPacket

                // TODO: 把执行结果输出。要区分是查询类还是修改类的SQL

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