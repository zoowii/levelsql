package com.zoowii.levelsql.engine.types

import com.zoowii.levelsql.engine.store.StoreSerializable
import com.zoowii.levelsql.engine.store.toBytes
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import com.zoowii.levelsql.engine.utils.compareBytes
import com.zoowii.levelsql.engine.utils.compareNodeKey
import java.io.ByteArrayOutputStream
import java.io.IOException

typealias DatumType = Int

object DatumTypes {
    val kindNull: DatumType = 0
    val kindInt64: DatumType = 1
    val kindString: DatumType = 2
    val kindText: DatumType = 3
    val kindBool: DatumType = 4
}

class Datum(var kind: DatumType, var intValue: Long? = null, var stringValue: String? = null,
            var boolValue: Boolean? = null) : StoreSerializable<Datum>, Comparable<Datum?> {

    override fun toString(): String {
        return when(kind) {
            DatumTypes.kindNull -> "null"
            DatumTypes.kindInt64 -> intValue.toString()
            DatumTypes.kindString -> stringValue.toString()
            DatumTypes.kindText -> stringValue.toString()
            DatumTypes.kindBool -> boolValue.toString()
            else -> "not supported datum kind $kind"
        }
    }

    override fun toBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(kind.toBytes())
        when(kind) {
            DatumTypes.kindNull -> {}
            DatumTypes.kindInt64 -> {
                out.write(intValue!!.toBytes())
            }
            DatumTypes.kindString -> {
                out.write(stringValue!!.toBytes())
            }
            DatumTypes.kindText -> {
                out.write(stringValue!!.toBytes())
            }
            DatumTypes.kindBool -> {
                out.write(boolValue!!.toBytes())
            }
            else -> throw IOException("not supported datum kind $kind toBytes")
        }
        return out.toByteArray()
    }

    override fun fromBytes(stream: ByteArrayStream): Datum {
        val kind: DatumType = stream.unpackInt32()
        this.kind = kind
        when(kind) {
            DatumTypes.kindNull -> {}
            DatumTypes.kindInt64 -> {
                this.intValue = stream.unpackInt64()
            }
            DatumTypes.kindString -> {
                this.stringValue = stream.unpackString()
            }
            DatumTypes.kindText -> {
                this.stringValue = stream.unpackString()
            }
            DatumTypes.kindBool -> {
                this.boolValue = stream.unpackBoolean()
            }
            else -> throw IOException("not supported datum kind $kind fromBytes")
        }
        return this
    }

    override fun compareTo(other: Datum?): Int {
        if(other==null) {
            return 1
        }
        if(kind != other.kind) {
            return kind - other.kind
        }
        return when(kind) {
            DatumTypes.kindNull -> 0
            DatumTypes.kindInt64 -> {
                (intValue!! - other.intValue!!).toInt()
            }
            DatumTypes.kindString -> {
                stringValue!!.compareTo(other.stringValue!!)
            }
            DatumTypes.kindText -> {
                stringValue!!.compareTo(other.stringValue!!)
            }
            DatumTypes.kindBool -> {
                boolValue!!.compareTo(other.boolValue!!)
            }
            else -> compareBytes(this.toBytes(), other.toBytes())
        }
    }
}