package com.zoowii.levelsql.sql.scanner

import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import java.math.BigDecimal

val firstReserved: Rune = 257 // 257是第一个非ASCII字符，用来代表保留关键字的类型。1-256的字符本身代表自己的token类型
val endOfStream: Rune = -1
val maxInt: Long = Long.MAX_VALUE
val maxUint8 = 1.shl(8) - 1

// SQL语法中的关键字
val reservedTokens = arrayOf("describe", "show", "create", "drop", "database", "table", "index",
        "select", "insert", "update", "delete", "alter", "and", "or", "where",
        "order", "by", "asc", "desc", "from", "join", "union", "left", "right", "inner", "outer", "full",
        "into", "default", "primary", "key", "auto_increment", "values", "set", "use",
        "explain", "on", "add", "column", "group", "limit", "true", "false",
        "not", "null",
        ">=", "<=", "!=", "<>",
        "<number>", "<int>", "<name>", "<string>", "<eof>")

private fun reservedTokenTypeByName(symbol: String): Rune {
    return reservedTokens.indexOf(symbol) + firstReserved
}

object TokenTypes {
    val tkDescribe: Rune = reservedTokenTypeByName("describe")
    val tkShow: Rune = reservedTokenTypeByName("show")
    val tkCreate: Rune = reservedTokenTypeByName("create")
    val tkDrop: Rune = reservedTokenTypeByName("drop")
    val tkDatabase: Rune = reservedTokenTypeByName("database")
    val tkTable: Rune = reservedTokenTypeByName("table")
    val tkIndex: Rune = reservedTokenTypeByName("index")
    val tkSelect: Rune = reservedTokenTypeByName("select")
    val tkInsert: Rune = reservedTokenTypeByName("insert")
    val tkUpdate: Rune = reservedTokenTypeByName("update")
    val tkDelete: Rune = reservedTokenTypeByName("delete")
    val tkAlter: Rune = reservedTokenTypeByName("alter")
    val tkAnd: Rune = reservedTokenTypeByName("and")
    val tkOr: Rune = reservedTokenTypeByName("or")
    val tkWhere: Rune = reservedTokenTypeByName("where")
    val tkOrder: Rune = reservedTokenTypeByName("order")
    val tkBy: Rune = reservedTokenTypeByName("by")
    val tkAsc: Rune = reservedTokenTypeByName("asc")
    val tkDesc: Rune = reservedTokenTypeByName("desc")
    val tkFrom: Rune = reservedTokenTypeByName("from")
    val tkJoin: Rune = reservedTokenTypeByName("join")
    val tkUnion: Rune = reservedTokenTypeByName("union")
    val tkLeft: Rune = reservedTokenTypeByName("left")
    val tkRight: Rune = reservedTokenTypeByName("right")
    val tkInner: Rune = reservedTokenTypeByName("inner")
    val tkOuter: Rune = reservedTokenTypeByName("outer")
    val tkFull: Rune = reservedTokenTypeByName("full")
    val tkInto: Rune = reservedTokenTypeByName("into")
    val tkDefault: Rune = reservedTokenTypeByName("default")
    val tkPrimary: Rune = reservedTokenTypeByName("primary")
    val tkKey: Rune = reservedTokenTypeByName("key")
    val tkAutoIncrement: Rune = reservedTokenTypeByName("auto_increment")
    val tkValues: Rune = reservedTokenTypeByName("values")
    val tkSet: Rune = reservedTokenTypeByName("set")
    val tkUse: Rune = reservedTokenTypeByName("use")
    val tkExplain: Rune = reservedTokenTypeByName("explain")
    val tkOn: Rune = reservedTokenTypeByName("on")
    val tkAdd: Rune = reservedTokenTypeByName("add")
    val tkColumn: Rune = reservedTokenTypeByName("column")
    val tkGroup: Rune = reservedTokenTypeByName("group")
    val tkLimit: Rune = reservedTokenTypeByName("limit")
    val tkTrue: Rune = reservedTokenTypeByName("true")
    val tkFalse: Rune = reservedTokenTypeByName("false")
    val tkNot: Rune = reservedTokenTypeByName("not")
    val tkNull: Rune = reservedTokenTypeByName("null")
    val tkGe: Rune = reservedTokenTypeByName(">=")
    val tkLe: Rune = reservedTokenTypeByName("<=")
    val tkNe: Rune = reservedTokenTypeByName("!=")
    val tkGL: Rune = reservedTokenTypeByName("<>") // less or great than, 等价于 !=
    val tkNumber: Rune = reservedTokenTypeByName("<number>")
    val tkInt: Rune = reservedTokenTypeByName("<int>")
    val tkName: Rune = reservedTokenTypeByName("<name>")
    val tkString: Rune = reservedTokenTypeByName("<string>")
    val tkEOS: Rune = reservedTokenTypeByName("<eof>")

    val reservedCount: Rune = tkGL - firstReserved + 1 // 保留关键字的数量

}



// t: 当前的symbol token或者字符
// i: 当前的整数
// n: 当前的浮点数
// s: 当前的字符串或者符号字面量
class Token(var t: Rune, var i: Long?=null, var n: BigDecimal?=null, var s: String="") {
    override fun toString(): String {
        return toString(t)
    }
    fun toString(t: Rune): String {
        when {
            t == TokenTypes.tkName || t == TokenTypes.tkString -> {
                return s
            }
            t == TokenTypes.tkInt -> {
                return "${i}"
            }
            t == TokenTypes.tkNumber -> {
                return "${n}"
            }
            t < firstReserved -> {
                return t.toChar().toString()
            }
            t < TokenTypes.tkEOS -> {
                return "'${reservedTokens[t - firstReserved]}'"
            }
        }
        return reservedTokens[t - firstReserved]
    }

    fun isLiteralValue(): Boolean {
        return listOf(TokenTypes.tkNull, TokenTypes.tkInt, TokenTypes.tkString,
                TokenTypes.tkTrue, TokenTypes.tkFalse).contains(t)
    }

    // 是否是二元表达式的运算符号
    fun isBinExprOperatorToken(): Boolean {
        val singleOps = listOf('+','-','*','/','%','=', '>', '<')
        for(op in singleOps) {
            if(t == op.toInt())
                return true
        }
        val otherOps = listOf(TokenTypes.tkGe, TokenTypes.tkLe, TokenTypes.tkNe, TokenTypes.tkGL, TokenTypes.tkAnd, TokenTypes.tkOr)
        for(op in otherOps) {
            if(t == op)
                return true
        }
        return false
    }

    fun getLiteralDatumValue(): Datum {
        return when(t) {
            TokenTypes.tkNull -> Datum(DatumTypes.kindNull)
            TokenTypes.tkInt -> Datum(DatumTypes.kindInt64, intValue = i)
            TokenTypes.tkString -> Datum(DatumTypes.kindString, stringValue = s)
            TokenTypes.tkTrue -> Datum(DatumTypes.kindBool, boolValue = true)
            TokenTypes.tkFalse -> Datum(DatumTypes.kindBool, boolValue = false)
            else -> Datum(DatumTypes.kindNull)
        }
    }
}