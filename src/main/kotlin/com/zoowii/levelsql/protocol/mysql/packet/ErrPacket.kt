package com.zoowii.levelsql.protocol.mysql.packet

import com.zoowii.levelsql.protocol.mysql.Capabilities
import com.zoowii.levelsql.protocol.mysql.MysqlPacket
import com.zoowii.levelsql.protocol.mysql.ProtoStreamReader
import com.zoowii.levelsql.protocol.mysql.ProtoStreamWriter
import java.io.IOException
import kotlin.experimental.and

class ErrPacket : MysqlPacket() {
    var errorCode: Short = 0
    var errorMessage: String? = null
    var sqlState: String? = null
    var sqlStateMarker: String? = null

    override fun read(reader: ProtoStreamReader) {
        val context = this.context!!

        if (reader.getInt1() and 0xFF.toByte() != 0xFF.toByte()) {
            throw IOException("invalid ERR packet")
        }

        errorCode = reader.getInt2()

        if (context.isCapabilitySet(Capabilities.PROTOCOL_41)) { //@formatter:off
            sqlStateMarker = reader.getFixedLengthString(1)
            sqlState = reader.getFixedLengthString(5)
            //@formatter:on
        }

        errorMessage = reader.getStringEOF()
    }

    override fun write(writer: ProtoStreamWriter) {
        val context = this.context!!

        writer.putInt1(0xFF.toByte())
        writer.putInt2(errorCode)

        if (context.isCapabilitySet(Capabilities.PROTOCOL_41)) {
            writer.putFixedLengthString(sqlStateMarker!!, 1)
            writer.putFixedLengthString(sqlState!!, 5)
        }

        writer.putFixedLengthString(errorMessage!!, errorMessage!!.length)
    }
}