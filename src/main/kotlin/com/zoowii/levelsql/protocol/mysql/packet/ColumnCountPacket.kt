package com.zoowii.levelsql.protocol.mysql.packet

import com.zoowii.levelsql.protocol.mysql.MysqlPacket
import com.zoowii.levelsql.protocol.mysql.ProtoStreamReader
import com.zoowii.levelsql.protocol.mysql.ProtoStreamWriter

class ColumnCountPacket : MysqlPacket() {
    var columnCount: Int = 0

    override fun read(reader: ProtoStreamReader) {
        this.columnCount = reader.readLength().toInt()
    }

    override fun write(writer: ProtoStreamWriter) {
        writer.putLengthEncodedInt(columnCount.toLong())
    }
}