package com.zoowii.levelsql.engine

import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.store.*
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import java.io.ByteArrayOutputStream


interface ColumnType {
    companion object {
        // ColumnType的序列化用8字节表示，前4字节表示类型(比如varchar)，后4字节表示限制长度(比如varchar(50)中的50)或0
        fun fromBytes(stream: ByteArrayStream): ColumnType {
            val baseType = stream.unpackInt32()
            val size = stream.unpackInt32()
            return when(baseType) {
                VarCharColumnType.baseTypeEnum() -> VarCharColumnType(size)
                IntColumnType.baseTypeEnum() -> IntColumnType()
                TextColumnType.baseTypeEnum() -> TextColumnType()
                BoolColumnType.baseTypeEnum() -> BoolColumnType()
                else -> throw DbException("invalid column type enum $baseType")
            }
        }
        fun serializeColumnType(columnType: ColumnType, size: Int): ByteArray {
            val out = ByteArrayOutputStream()
            out.write(columnType.baseTypeEnum().toBytes())
            out.write(size.toBytes())
            return out.toByteArray()
        }
    }
    fun baseTypeEnum(): Int
    fun toBytes(): ByteArray
}

class VarCharColumnType(val size: Int) : ColumnType {
    companion object {
        fun baseTypeEnum(): Int = 1
    }
    override fun baseTypeEnum(): Int {
        return 1
    }

    override fun toBytes(): ByteArray {
        return ColumnType.serializeColumnType(this, size)
    }

    override fun toString(): String {
        return "varchar($size)"
    }
}

class IntColumnType : ColumnType {
    companion object {
        fun baseTypeEnum(): Int = 2
    }
    override fun baseTypeEnum(): Int {
        return 2
    }
    override fun toBytes(): ByteArray {
        return ColumnType.serializeColumnType(this, 0)
    }

    override fun toString(): String {
        return "int"
    }
}

class TextColumnType : ColumnType {
    companion object {
        fun baseTypeEnum(): Int = 3
    }
    override fun baseTypeEnum(): Int {
        return 3
    }
    override fun toBytes(): ByteArray {
        return ColumnType.serializeColumnType(this, 0)
    }

    override fun toString(): String {
        return "text"
    }
}

class BoolColumnType : ColumnType {
    companion object {
        fun baseTypeEnum(): Int = 4
    }
    override fun baseTypeEnum(): Int {
        return 4
    }
    override fun toBytes(): ByteArray {
        return ColumnType.serializeColumnType(this, 0)
    }

    override fun toString(): String {
        return "bool"
    }
}

class TableColumnDefinition(val name: String, val columnType: ColumnType, val nullable: Boolean = true) {
    override fun toString(): String {
        return "$name $columnType" + (if(nullable) "" else " not null")
    }

    companion object {
        fun fromBytes(stream: ByteArrayStream): TableColumnDefinition {
            val name = stream.unpackString()
            val columnType = ColumnType.fromBytes(stream)
            val nullable = stream.unpackBoolean()
            return TableColumnDefinition(name, columnType, nullable)
        }
    }

    fun toBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(name.toBytes())
        out.write(columnType.toBytes())
        out.write(nullable.toBytes())
        return out.toByteArray()
    }
}