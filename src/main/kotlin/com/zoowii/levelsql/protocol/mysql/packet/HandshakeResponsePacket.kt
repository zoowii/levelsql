package com.zoowii.levelsql.protocol.mysql.packet

import com.zoowii.levelsql.protocol.mysql.Capabilities
import com.zoowii.levelsql.protocol.mysql.MysqlPacket
import com.zoowii.levelsql.protocol.mysql.ProtoStreamReader
import com.zoowii.levelsql.protocol.mysql.ProtoStreamWriter
import java.util.*

// HandshakeResponse41
class HandshakeResponsePacket : MysqlPacket() {
    private var authPluginName: String? = null
    private var authResponse: String? = null
    private var capabilities: Long = 0
    private var characterSet: Byte = 0
    private var connectAttributes: MutableMap<String, String>? = null
    private var database: String? = null
    private var maxPacketSize = 0
    private var username: String? = null

    private fun isCapabilitySet(capability: Long): Boolean {
        return (capabilities and capability) != 0.toLong()
    }


    override fun read(reader: ProtoStreamReader) {
//@formatter:off
        //@formatter:off
        capabilities = reader.getInt4().toLong()
        maxPacketSize = reader.getInt4()
        characterSet = reader.getInt1()
        reader.skip(23)
        //@formatter:on

        username = reader.getNullTerminatedString()


        if (isCapabilitySet(Capabilities.PLUGIN_AUTH_LENENC_DATA)) {
            authResponse = reader.getLengthEncodedString()
        } else {
            val length = reader.getInt1().toInt()
            authResponse = reader.getFixedLengthString(length)
        }

        if (isCapabilitySet(Capabilities.CONNECT_WITH_DB)) {
            database = reader.getNullTerminatedString()
        }

        if (isCapabilitySet(Capabilities.PLUGIN_AUTH)) {
            authPluginName = reader.getNullTerminatedString()
        }

        if (isCapabilitySet(Capabilities.CONNECT_ATTRS)) {
            val size = reader.getLengthEncodedInt().toInt()
            val connectAttributes: MutableMap<String, String> = HashMap()
            for (i in 0 until size) {
                connectAttributes[reader.getLengthEncodedString()] = reader.getLengthEncodedString()
            }
            this.connectAttributes = connectAttributes
        }
    }

    override fun write(writer: ProtoStreamWriter) {
//@formatter:off
        //@formatter:off
        writer.putInt4(capabilities as Int)
        writer.putInt4(maxPacketSize)
        writer.putInt1(characterSet)
        writer.skip(19)
        //@formatter:on

        //@formatter:on
        if (!isCapabilitySet(Capabilities.CLIENT_MYSQL)) {
            writer.putInt4((capabilities shr 16) as Int)
        } else {
            writer.skip(4)
        }

        writer.putNullTerminatedString(username!!)

        if (isCapabilitySet(Capabilities.PLUGIN_AUTH_LENENC_DATA)) {
            writer.putLengthEncodedString(authResponse!!)
        } else if (isCapabilitySet(Capabilities.SECURE_CONNECTION)) {
            val length: Int = authResponse!!.length
            writer.putInt1(length.toByte())
            writer.putFixedLengthString(authResponse!!, length)
        } else {
            writer.skip(1)
        }

        if (isCapabilitySet(Capabilities.CONNECT_WITH_DB)) {
            writer.putNullTerminatedString(database!!)
        }

        if (isCapabilitySet(Capabilities.PLUGIN_AUTH)) {
            writer.putNullTerminatedString(authPluginName!!)
        }

        if (isCapabilitySet(Capabilities.CONNECT_ATTRS)) {
            val connectAttributes = this.connectAttributes
            val size = connectAttributes!!.size
            writer.putLengthEncodedInt(size.toLong())
            for (key in connectAttributes!!.keys) {
                writer.putLengthEncodedString(key)
                writer.putLengthEncodedString(connectAttributes!![key]!!)
            }
        }
    }

    override fun toString(): String {
        return "handshakeResponse{username=$username, databae=$database}"
    }
}