package com.zoowii.levelsql.protocol.mysql.packet

import com.zoowii.levelsql.protocol.mysql.MysqlPacket
import com.zoowii.levelsql.protocol.mysql.ProtoStreamReader
import com.zoowii.levelsql.protocol.mysql.ProtoStreamWriter
import kotlin.experimental.and


class ColumnDefinitionPacket : MysqlPacket() {
    private val DEFAULT_CATALOG = "def".toByteArray()
    private val NEXT_LENGTH: Byte = 0x0c
    private val FILLER = byteArrayOf(0, 0)

    var catalog: ByteArray = DEFAULT_CATALOG // always "def"

    var schema: ByteArray? = null // schema-name, length-encoded string
    var table: ByteArray? = null // virtual table-name
    var orgTable: ByteArray? = null // physical table-name
    var name: ByteArray? = null // virtual column name
    var orgName: ByteArray? = null // physical column name
    var nextLength = NEXT_LENGTH // length of the following fields (always 0x0c)

    var charsetSet = 0 // is the column character set and is defined in Protocol::CharacterSet.
    var length: Long = 0 // maximum length of the field
    var type = 0 // type of the column as defined in Column Type
    var flags = 0
    // max shown decimal digits.
    // 0x00 for integers and static strings
    // 0x1f for dynamic strings, double, float
    // 0x00 to 0x51 for decimals
    var decimals: Byte = 0
    var filler: ByteArray = FILLER
    var defaultValues: ByteArray? = null

    override fun read(reader: ProtoStreamReader) {
        this.catalog = reader.readBytesWithLength()!!
        this.schema = reader.readBytesWithLength()
        this.table = reader.readBytesWithLength()
        this.orgTable = reader.readBytesWithLength()
        this.name = reader.readBytesWithLength()
        this.orgName = reader.readBytesWithLength()
        this.nextLength = reader.getInt1()
        this.charsetSet = reader.getInt2().toInt()
        this.length = reader.getInt4().toLong()
        this.type = reader.getInt1().toInt() and 0xff
        this.flags = reader.getInt2().toInt()
        this.decimals = reader.getInt1()
        this.filler = reader.readBytes(2)!!
        if(reader.hasMore()) {
            this.defaultValues = reader.readBytesWithLength()
        }
    }

    override fun write(writer: ProtoStreamWriter) {
        val nullValue = 0.toByte()
        writer.writeWithLength(catalog, nullValue)
        writer.writeWithLength(schema, nullValue)
        writer.writeWithLength(table, nullValue)
        writer.writeWithLength(orgTable, nullValue)
        writer.writeWithLength(name, nullValue)
        writer.writeWithLength(orgName, nullValue)
        writer.putInt1(NEXT_LENGTH)
        writer.putInt2(charsetSet.toShort())
        writer.putInt4(length.toInt())
        writer.putInt1((type and 0xff).toByte())
        writer.putInt2(flags.toShort())
        writer.putInt1(decimals)
        writer.putBytes(FILLER)
        if(defaultValues!=null) {
            writer.writeWithLength(defaultValues!!)
        }
    }
}