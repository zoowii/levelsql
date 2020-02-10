package com.zoowii.levelsql.protocol.mysql.packet

import com.zoowii.levelsql.protocol.mysql.MysqlCommandType
import com.zoowii.levelsql.protocol.mysql.ProtoStreamReader
import com.zoowii.levelsql.protocol.mysql.ProtoStreamWriter

class SleepPacket : MysqlCommandType() {
    override fun read(reader: ProtoStreamReader) {

    }

    override fun write(writer: ProtoStreamWriter) {

    }
}