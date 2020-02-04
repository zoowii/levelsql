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

    // 计算本row应用到表达式{expr}后计算得到的值, @param headerNames 是本row各数据对应的header name
    fun calculateExpr(expr: Expr, headerNames: List<String>): Datum {
        when(expr.javaClass) {
            BinOpExpr::class.java -> {
                expr as BinOpExpr
                val leftValue = this.calculateExpr(expr.left, headerNames)
                val rightValue = this.calculateExpr(expr.right, headerNames)
                val op = expr.op
                // TODO: 为了简化实现，数值计算目前只接受整数
                when(op.t) {
                    '>'.toInt() -> {
                        if(leftValue.kind != DatumTypes.kindInt64
                                || rightValue.kind != DatumTypes.kindInt64) {
                            throw SQLException("sql math op now only support integer")
                        }
                        return Datum(DatumTypes.kindBool, boolValue =  leftValue.intValue!! > rightValue.intValue!!)
                    }
                    '<'.toInt() -> {
                        if(leftValue.kind != DatumTypes.kindInt64
                                || rightValue.kind != DatumTypes.kindInt64) {
                            throw SQLException("sql math op now only support integer")
                        }
                        return Datum(DatumTypes.kindBool, boolValue =  leftValue.intValue!! < rightValue.intValue!!)
                    }
                    '='.toInt() -> {
                        return Datum(DatumTypes.kindBool,
                                boolValue =  (leftValue.kind == rightValue.kind && leftValue.toString() == rightValue.toString()))
                    }
                    TokenTypes.tkGe -> {
                        // >=
                        if(leftValue.kind != DatumTypes.kindInt64
                                || rightValue.kind != DatumTypes.kindInt64) {
                            throw SQLException("sql math op now only support integer")
                        }
                        return Datum(DatumTypes.kindBool, boolValue =  leftValue.intValue!! >= rightValue.intValue!!)
                    }
                    TokenTypes.tkGL -> {
                        // <>
                        return Datum(DatumTypes.kindBool,
                                boolValue =  (leftValue.kind != rightValue.kind || leftValue.toString() != rightValue.toString()))
                    }
                    TokenTypes.tkNe -> {
                        // !=
                        return Datum(DatumTypes.kindBool,
                                boolValue =  (leftValue.kind != rightValue.kind || leftValue.toString() != rightValue.toString()))
                    }
                    TokenTypes.tkLe -> {
                        // <=
                        if(leftValue.kind != DatumTypes.kindInt64
                                || rightValue.kind != DatumTypes.kindInt64) {
                            throw SQLException("sql math op now only support integer")
                        }
                        return Datum(DatumTypes.kindBool, boolValue =  leftValue.intValue!! <= rightValue.intValue!!)
                    }
                    else -> throw SQLException("not supported op $op in expr")
                }
            }
            TokenExpr::class.java -> {
                expr as TokenExpr
                val token = expr.token
                return when {
                    token.t == TokenTypes.tkName -> getItem(headerNames, token.s)
                    token.isLiteralValue() -> token.getLiteralDatumValue()
                    else -> throw SQLException("not supported token $token in expr")
                }
            }
            else -> {
                throw SQLException("not supported CondExpr type ${expr.javaClass}")
            }
        }
    }

    // 本row是否满足条件表达式{cond}, @param headerNames 是本row各数据对应的header name
    fun matchCondExpr(cond: Expr, headerNames: List<String>): Boolean {
        val exprValue = this.calculateExpr(cond, headerNames)
        return exprValue.kind == DatumTypes.kindBool && exprValue.boolValue!!
    }
}