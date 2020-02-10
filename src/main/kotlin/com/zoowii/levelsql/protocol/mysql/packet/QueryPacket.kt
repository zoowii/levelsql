package com.zoowii.levelsql.protocol.mysql.packet

import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.protocol.mysql.MysqlCommandType
import com.zoowii.levelsql.protocol.mysql.ProtoStreamReader
import com.zoowii.levelsql.protocol.mysql.ProtoStreamWriter

class QueryPacket : MysqlCommandType() {
    private val log = logger()

    var message: ByteArray = byteArrayOf()

    override fun read(reader: ProtoStreamReader) {
        message = reader.readBytes()
        log.debug("query packet: ${String(message)}")
    }

    override fun write(writer: ProtoStreamWriter) {
        writer.putBytes(message)
    }
}