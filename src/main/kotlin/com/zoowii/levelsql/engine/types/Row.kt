package com.zoowii.levelsql.engine.types

import com.zoowii.levelsql.engine.store.StoreSerializable
import com.zoowii.levelsql.engine.store.toBytes
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import java.io.ByteArrayOutputStream

class Row : StoreSerializable<Row> {
    var data = listOf<Datum>()

    override fun toString(): String {
        return data.joinToString(",\t")
    }

    override fun toBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(data.size.toBytes())
        data.map {
            out.write(it.toBytes())
        }
        return out.toByteArray()
    }

    override fun fromBytes(stream: ByteArrayStream): Row {
        val count = stream.unpackInt32()
        val items = mutableListOf<Datum>()
        (0 until count).map {
            items.add(Datum(DatumTypes.kindNull).fromBytes(stream))
        }
        this.data = items
        return this
    }
}