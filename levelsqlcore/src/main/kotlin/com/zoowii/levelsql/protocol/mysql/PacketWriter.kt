package com.zoowii.levelsql.protocol.mysql

import java.io.Closeable
import java.io.IOException


internal class PacketWriter(private val context: Context) : Closeable {
    @Throws(IOException::class)
    override fun close() {
        context.outputStream.close()
    }

    @Throws(IOException::class)
    fun write(packet: MysqlPacket) {
        var writer = ProtoStreamWriter()
        packet.write(writer)
        val payload: ByteArray = writer.build()
        writer = ProtoStreamWriter(4)
        writer.putInt3(payload.size)
        writer.putInt1(packet.sequenceId)
        val header: ByteArray = writer.build()
        val out = context.outputStream
        out.write(header)
        if(MysqlCommandType::class.java.isAssignableFrom(packet.javaClass)) {
            packet as MysqlCommandType
            out.write(packet.commandType.toInt())
        }
        out.write(payload)
        out.flush()
    }

}
