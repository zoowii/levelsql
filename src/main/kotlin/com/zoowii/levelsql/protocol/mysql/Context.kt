package com.zoowii.levelsql.protocol.mysql

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream


class Context(val inputStream: InputStream, val outputStream: OutputStream) {
    private val reader = PacketReader(this)
    private val writer = PacketWriter(this)

    var capabilities: Long = 0
    var status: Int = 0

    private var seqIdGen: Byte = 0

    fun setLastSeqId(seqId: Byte) {
        seqIdGen = (seqId + 1).toByte()
    }
    
    fun nextSeqId(): Byte {
        return seqIdGen++
    }

    fun <T : MysqlPacket?> create(clazz: Class<T>, sequenceId: Byte): T {
        return try {
            val packet = clazz.getDeclaredConstructor().newInstance() ?: throw IOException("create packet ${clazz.canonicalName} error")
            packet.sequenceId = sequenceId
            packet.context = this
            packet
        } catch (e: Exception) {
            throw IOException()
        }
    }


    @Throws(IOException::class)
    fun receiveCmdType(): MysqlCommandType {
        return reader.read(null, true) as MysqlCommandType
    }

    @Throws(IOException::class)
    fun <T : MysqlPacket> receive(cls: Class<T>, isCmdType: Boolean): MysqlPacket {
        return reader.read(cls, isCmdType)
    }

    @Throws(IOException::class)
    fun send(packet: MysqlPacket) {
        writer.write(packet)
    }

    fun isCapabilitySet(capability: Long): Boolean {
        return capabilities and capability != 0L
    }

    fun isStatusFlagSet(statusFlag: Int): Boolean {
        return status and statusFlag !== 0
    }

    fun setCapabilityFlag(capability: Int) {
        capabilities = capabilities or capability.toLong()
    }

    fun setStatusFlag(status: Int) {
        this.status = this.status or status
    }

}