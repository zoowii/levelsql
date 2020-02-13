package com.zoowii.levelsql.protocol.mysql.packet

import com.zoowii.levelsql.protocol.mysql.MysqlPacket
import com.zoowii.levelsql.protocol.mysql.ProtoStreamReader
import com.zoowii.levelsql.protocol.mysql.ProtoStreamWriter

// https://dev.mysql.com/doc/internals/en/com-query-response.html#text-resultset
class ResultSetRowPacket : MysqlPacket() {
    private val NULL_MARK = 251.toByte()

    var columnCount: Int = 0
    var columnValues: MutableList<ByteArray?> = mutableListOf()


    override fun read(reader: ProtoStreamReader) {
        for (i in 0 until columnCount) {
            columnValues.add(reader.readBytesWithLength())
        }
    }

    override fun write(writer: ProtoStreamWriter) {
        for (i in 0 until columnCount) {
            val fv = columnValues[i]
            if (fv == null) {
                writer.putInt1(NULL_MARK)
            } else {
                writer.putLengthEncodedInt(fv.size.toLong())
                writer.putBytes(fv)
            }
        }
    }
}