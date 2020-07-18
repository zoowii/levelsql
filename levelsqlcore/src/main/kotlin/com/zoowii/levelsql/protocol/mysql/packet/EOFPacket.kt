package com.zoowii.levelsql.protocol.mysql.packet

import com.zoowii.levelsql.protocol.mysql.MysqlPacket
import com.zoowii.levelsql.protocol.mysql.ProtoStreamReader
import com.zoowii.levelsql.protocol.mysql.ProtoStreamWriter

// http://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
class EOFPacket : MysqlPacket() {
    var header = 0xfe.toByte()
    var warningCount = 0
    var status = 2

    override fun read(reader: ProtoStreamReader) {
        header = reader.getInt1()
        warningCount = reader.getInt2().toInt()
        status = reader.getInt2().toInt()
    }

    override fun write(writer: ProtoStreamWriter) {
        writer.putInt1(header)
        writer.putInt2(warningCount.toShort())
        writer.putInt2(status.toShort())
    }
}