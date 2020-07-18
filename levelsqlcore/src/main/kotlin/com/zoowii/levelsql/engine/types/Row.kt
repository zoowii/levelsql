package com.zoowii.levelsql.engine.types

import com.zoowii.levelsql.engine.store.StoreSerializable
import com.zoowii.levelsql.engine.store.toBytes
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import com.zoowii.levelsql.sql.ast.BinOpExpr
import com.zoowii.levelsql.sql.ast.Expr
import com.zoowii.levelsql.sql.ast.TokenExpr
import com.zoowii.levelsql.sql.scanner.TokenTypes
import java.io.ByteArrayOutputStream
import java.sql.SQLException

class Row : StoreSerializable<Row> {
    companion object {
        // 使用Percolator算法实现分布式事务时，一行数据有可选的两列隐藏列L列和W列，
        // L列用来加锁startTs+sessionId+optional-primaryRowId，W列记录commitTs
        // 因为Row记录中只有值没有列信息，但是Table操作Row的时候能知道table中有多少列，
        // 所以可以给row记录最后增加2列，如果只有L列则是L+W(null),如果只有W列则是L(null)+W
        const val LockColumnName = "@L"
        const val lockCommitColumnName = "@W"
    }

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

    fun getItem(headerNames: List<String>, name: String): Datum {
        val idx = headerNames.indexOf(name)
        if(idx < 0 || idx >= data.size) {
            return Datum(DatumTypes.kindNull)
        }
        return data[idx]
    }

    fun setItemByIndex(index: Int, value: Datum) {
        val mList = data.toMutableList()
        mList[index] = value
        this.data = mList
    }

    // 本row是否满足条件表达式{cond}, @param headerNames 是本row各数据对应的header name
    fun matchCondExpr(cond: Expr, headerNames: List<String>): Boolean {
        val exprValue = cond.eval(Chunk().replaceRows(listOf(this)), headerNames)[0]
        return exprValue.kind == DatumTypes.kindBool && exprValue.boolValue!!
    }
}