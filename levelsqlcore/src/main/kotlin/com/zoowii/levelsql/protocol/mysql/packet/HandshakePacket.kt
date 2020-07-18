package com.zoowii.levelsql.protocol.mysql.packet

import com.alibaba.fastjson.JSON
import com.zoowii.levelsql.protocol.mysql.Capabilities
import com.zoowii.levelsql.protocol.mysql.MysqlPacket
import com.zoowii.levelsql.protocol.mysql.ProtoStreamReader
import com.zoowii.levelsql.protocol.mysql.ProtoStreamWriter


// HandshakeV10 packet
class HandshakePacket : MysqlPacket() {

    var protocolVersion: Byte = 0 // always 10 for handshakeV10
    var serverVersion: String? = null
    var connectionId: Int = 0
    // 0x00 byte, terminating the first part of a scramble
    var capabilities: Long = 0
    var characterSet: Int = 0
    var serverStatus: Int = 0
    var authPluginData: String? = null
    // string[10] all 0s for reserved
    // Rest of the plugin provided data (scramble), $len=MAX(13, length of auth-plugin-data - 8)
    var authPluginName: String? = null

    override fun read(reader: ProtoStreamReader) {
        //@formatter:off
        protocolVersion = reader.getInt1()
        serverVersion = reader.getNullTerminatedString()
        connectionId = reader.getInt4()
        //@formatter:on

        //@formatter:on
        if (protocolVersion < 10) {
            authPluginData = reader.getNullTerminatedString()
            return
        }

        //@formatter:off
        //@formatter:off
        authPluginData = reader.getFixedLengthString(8)
        reader.skip(1)
        capabilities = reader.getInt2().toLong()
        //@formatter:on

        //@formatter:on
        if (!reader.hasMore()) {
            return
        }

        //@formatter:off
        //@formatter:off
        characterSet = reader.getInt1().toInt()
        serverStatus = reader.getInt2().toInt()
        capabilities = (reader.getInt2().toInt() shl 16).toLong() or capabilities
        //@formatter:on

        //@formatter:on
        var length = 0

        if (isCapabilitySet(Capabilities.PLUGIN_AUTH)) {
            length = reader.getInt1().toInt()
        } else {
            reader.skip(1)
        }

        reader.skip(6)

        if (isCapabilitySet(Capabilities.CLIENT_MYSQL)) {
            reader.skip(4)
        } else {
            capabilities = (reader.getInt4().toLong() shl 32 or capabilities)
        }

        if (isCapabilitySet(Capabilities.SECURE_CONNECTION)) {
            authPluginData = authPluginData + reader.getFixedLengthString(Math.max(12, length))
            reader.skip(1)
        }

        if (isCapabilitySet(Capabilities.PLUGIN_AUTH)) {
            authPluginName = reader.getNullTerminatedString()
        }
    }

    private fun isCapabilitySet(capability: Long): Boolean {
        return (capabilities and capability) != 0.toLong()
    }

    override fun write(writer: ProtoStreamWriter) {
        //@formatter:off
        //@formatter:off
        writer.putInt1(protocolVersion.toByte())
        writer.putNullTerminatedString(serverVersion!!)
        writer.putInt4(connectionId)
        //@formatter:on

        //@formatter:on
        if (protocolVersion < 10) {
            writer.putNullTerminatedString(authPluginData!!)
            return
        }

        //@formatter:off
        //@formatter:off
        writer.putFixedLengthString(authPluginData!!.substring(0, 8), 8)
        writer.skip(1)
        writer.putInt2(capabilities.toShort())
        writer.putInt1(characterSet.toByte())
        writer.putInt2(serverStatus.toShort())
        writer.putInt2((capabilities shr 16).toShort())
        //@formatter:on

        //@formatter:on
        if (isCapabilitySet(Capabilities.PLUGIN_AUTH)) {
            writer.putInt1((authPluginData!!.length - 8).toByte())
        } else {
            writer.putInt1(0.toByte())
        }

        writer.skip(6)

        if (isCapabilitySet(Capabilities.CLIENT_MYSQL)) {
            writer.skip(4)
        } else {
            writer.putInt4((capabilities shr 32) .toInt())
        }

        if (isCapabilitySet(Capabilities.SECURE_CONNECTION)) {
            writer.putNullTerminatedString(authPluginData!!.substring(8))
        }

        if (isCapabilitySet(Capabilities.PLUGIN_AUTH)) {
            writer.putNullTerminatedString(authPluginName!!)
        }
    }

}