package com.zoowii.levelsql.protocol.mysql.packet

import com.zoowii.levelsql.protocol.mysql.*
import java.io.IOException

class OkPacket : MysqlPacket() {
    var affectedRows: Long = 0
    var info: String? = ""
    var lastInsertId: Long = 0
    var sessionStateInfo: String? = null
    var statusFlags: Short = 0
    var warnings: Short = 0

    override fun read(reader: ProtoStreamReader) {
        val header = reader.getInt1().toInt()

        if (header != 0x00 && header != 0xFE) {
            throw IOException("ok packet header invalid")
        }

        val context = this.context ?: throw IOException("context not set")

        affectedRows = reader.getLengthEncodedInt()
        lastInsertId = reader.getLengthEncodedInt()

        if (context.isCapabilitySet(Capabilities.PROTOCOL_41)) { //@formatter:off
            statusFlags = reader.getInt2()
            warnings = reader.getInt2()
            //@formatter:on
        } else if (context.isCapabilitySet(Capabilities.TRANSACTIONS)) {
            statusFlags = reader.getInt2()
        }

        if (context.isCapabilitySet(Capabilities.CLIENT_SESSION_TRACK)) {
            info = reader.getLengthEncodedString()
            if (context.isStatusFlagSet(ServerStatus.SESSION_STATE_CHANGED)) {
                sessionStateInfo = reader.getLengthEncodedString()
            }
        } else {
            info = reader.getStringEOF()
        }
    }

    override fun write(writer: ProtoStreamWriter) {
        val context = context ?: throw IOException("context not set")

        //@formatter:off
        //@formatter:off
        writer.putInt1(0x00.toByte())
        writer.putLengthEncodedInt(affectedRows)
        writer.putLengthEncodedInt(lastInsertId)
        //@formatter:on

        //@formatter:on
        if (context.isCapabilitySet(Capabilities.PROTOCOL_41)) {
            writer.putInt2(statusFlags)
            writer.putInt2(warnings)
        } else if (context.isCapabilitySet(Capabilities.TRANSACTIONS)) {
            writer.putInt2(statusFlags)
        }

        if (context.isCapabilitySet(Capabilities.CLIENT_SESSION_TRACK)) {
            writer.putLengthEncodedString(info!!)
            if (context.isStatusFlagSet(ServerStatus.SESSION_STATE_CHANGED)) {
                writer.putLengthEncodedString(sessionStateInfo!!)
            }
        } else {
            writer.putFixedLengthString(info!!, info!!.length)
        }
    }
}