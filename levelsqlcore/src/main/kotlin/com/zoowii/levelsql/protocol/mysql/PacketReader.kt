package com.zoowii.levelsql.protocol.mysql

import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.protocol.mysql.packet.QueryPacket
import com.zoowii.levelsql.protocol.mysql.packet.QuitPacket
import com.zoowii.levelsql.protocol.mysql.packet.SleepPacket
import java.io.Closeable
import java.io.IOException

class PacketReader(private val context: Context) : Closeable {
    private val log = logger()

    override fun close() {
        context.inputStream.close()
    }

    @Throws(IOException::class)
    fun read(receiveCls: Class<*>?, isCmdType: Boolean): MysqlPacket {
        val input = context.inputStream
        val header = ByteArray(4)
        input.read(header, 0, header.size)
        val headerReader = ProtoStreamReader(header)
        val length = headerReader.getInt3()
        val seqId = headerReader.getInt1()

        var cmdFlag: Byte = 0
        val cls: Class<*>
        if(receiveCls != null) {
            cls = receiveCls
        } else {
            if(!isCmdType) {
                throw IOException("need receive command type")
            }
            cmdFlag = input.read().toByte()
            if(cmdFlag == (-1).toByte()) {
                return QuitPacket()
            }
            cls = when(cmdFlag) {
                MysqlPacket.CMD_SLEEP -> SleepPacket::class.java
                MysqlPacket.CMD_QUIT -> QuitPacket::class.java
                MysqlPacket.CMD_QUERY -> QueryPacket::class.java
                else -> throw IOException("not implemented packet type for cmd type $cmdFlag")
            }
        }
        log.debug("receiving packet ${cls.canonicalName}")
        val packet = cls.getDeclaredConstructor().newInstance() as MysqlPacket
        packet.context = context
        packet.packetLength = length
        packet.sequenceId = seqId
        if(isCmdType) {
            packet as MysqlCommandType
            packet.commandType = cmdFlag
        }

        val packetBody = ByteArray(length)
        input.read(packetBody, 0, length)
        val bodyReader = ProtoStreamReader(packetBody)
        packet.read(bodyReader)
        return packet
    }
}