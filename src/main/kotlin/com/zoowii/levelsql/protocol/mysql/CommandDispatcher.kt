package com.zoowii.levelsql.protocol.mysql

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.protocol.mysql.field.FieldTypes
import com.zoowii.levelsql.protocol.mysql.packet.*

class CommandDispatcher(private val server: MysqlServer) {
    private val log = logger()
    fun dispatchCommand(context: Context, packet: MysqlCommandType): List<MysqlPacket> {
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
                // 把执行结果输出。要区分是查询类还是修改类的SQL

                // https://dev.mysql.com/doc/internals/en/com-query-response.html#text-resultset
                // 查询类SQL返回结构包括多个packets的返回
                // 1. column_count和column_count个列定义的packets
                // 2. If the CLIENT_DEPRECATE_EOF client capability flag is not set, EOF_Packet
                // 3. 若干个ResultsetRow packets,每行一个
                // 4. EOF packet或者中途出错返回 ERR packet
                if(sqlResultSet.columns.isEmpty()) {
                    // 非查询类SQL的返回结果
                    val ok = context.create(OkPacket::class.java, context.nextSeqId())
                    ok.info = "execute successfully"
                    ok.affectedRows = 0 // TODO: 修改ok.affectedRows
                    return listOf(ok)
                }
                val columnCountPacket = context.create(ColumnCountPacket::class.java, context.nextSeqId())
                columnCountPacket.columnCount = sqlResultSet.columns.size

                val columnDefPackets = sqlResultSet.columns.map {
                    val columnName = it
                    val columnDefPacket = context.create(ColumnDefinitionPacket::class.java, context.nextSeqId())
                    columnDefPacket.name = columnName.toByteArray()
                    columnDefPacket.schema = "".toByteArray()
                    columnDefPacket.table = "".toByteArray()
                    columnDefPacket.orgName = "".toByteArray()
                    columnDefPacket.orgTable = "".toByteArray()
                    columnDefPacket.catalog = "".toByteArray()
                    columnDefPacket.charsetSet = 33 // utf8 charset index is 33
                    // TODO: 字段的类型，表，长度等信息. 目前所有列都当字符串返回给客户端
                    columnDefPacket.type = FieldTypes.VARCHAR
                    columnDefPacket.length = 100
                    columnDefPacket.flags = 1
                    columnDefPacket.decimals = 0
                    columnDefPacket
                }
                val columnDefsEofPackets = if(context.isCapabilitySet(Capabilities.CLIENT_DEPRECATE_EOF))
                    listOf<MysqlPacket>()
                else
                    listOf(context.create(EOFPacket::class.java, context.nextSeqId()))

                val rowPackets = sqlResultSet.chunk.rows.map {
                    val row = it
                    val resultSetPacket = context.create(ResultSetRowPacket::class.java, context.nextSeqId())
                    resultSetPacket.columnCount = row.data.size
                    row.data.map { col ->
                        resultSetPacket.columnValues.add(col.toString().toByteArray()) // TODO: row的datum要转换成mysql的行的bytes格式
                    }
                    resultSetPacket
                }
                val eofPacket = context.create(EOFPacket::class.java, context.nextSeqId())

                return listOf(columnCountPacket) + columnDefPackets + columnDefsEofPackets + rowPackets + eofPacket
            }
            else -> {
                log.error("not implemented processor for command type ${packet.javaClass.canonicalName}")
                val err = context.create(ErrPacket::class.java, context.nextSeqId())
                err.errorMessage = "not implemented processor for command type ${packet.javaClass.canonicalName}"
                err.errorCode = 10001
                err.sqlState = "10001"
                err.sqlStateMarker = "10001"
                return listOf(err)
            }
        }
    }
}