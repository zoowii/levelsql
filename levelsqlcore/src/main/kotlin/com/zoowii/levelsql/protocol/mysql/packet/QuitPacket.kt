package com.zoowii.levelsql.protocol.mysql.packet

import com.alibaba.fastjson.JSON
import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.protocol.mysql.MysqlCommandType
import com.zoowii.levelsql.protocol.mysql.ProtoStreamReader
import com.zoowii.levelsql.protocol.mysql.ProtoStreamWriter

class QuitPacket : MysqlCommandType() {
    private val log = logger()

    private var payload: Byte = 0

    override fun read(reader: ProtoStreamReader) {
        payload = reader.getInt1()
        log.debug("quit packet payload $payload")
    }

    override fun write(writer: ProtoStreamWriter) {
        writer.putInt1(payload)
    }
}