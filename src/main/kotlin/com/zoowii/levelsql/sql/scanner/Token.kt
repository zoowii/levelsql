package com.zoowii.levelsql.sql.scanner

import java.math.BigDecimal

val firstReserved: Rune = 257 // 257是第一个非ASCII字符，用来代表保留关键字的类型。1-256的字符本身代表自己的token类型
val endOfStream: Rune = -1
val maxInt: Long = Long.MAX_VALUE
val maxUint8 = 1.shl(8) - 1

object TokenTypes {
    val tkDescribe: Rune = 0 + firstReserved
    val tkShow: Rune = 1 + firstReserved
    val tkCreate: Rune = 2 + firstReserved
    val tkDrop: Rune = 3 + firstReserved
    val tkDatabase: Rune = 4 + firstReserved
    val tkTable: Rune = 5 + firstReserved
    val tkIndex: Rune = 6 + firstReserved
    val tkSelect: Rune = 7 + firstReserved
    val tkInsert: Rune = 8 + firstReserved
    val tkUpdate: Rune = 9 + firstReserved
    val tkDelete: Rune = 10 + firstReserved
    val tkAlter: Rune = 11 + firstReserved
    val tkAnd: Rune = 12 + firstReserved
    val tkOr: Rune = 13 + firstReserved
    val tkWhere: Rune = 14 + firstReserved
    val tkOrder: Rune = 15 + firstReserved
    val tkBy: Rune = 16 + firstReserved
    val tkAsc: Rune = 17 + firstReserved
    val tkDesc: Rune = 18 + firstReserved
    val tkFrom: Rune = 19 + firstReserved
    val tkJoin: Rune = 20 + firstReserved
    val tkUnion: Rune = 21 + firstReserved
    val tkLeft: Rune = 22 + firstReserved
    val tkRight: Rune = 23 + firstReserved
    val tkInner: Rune = 24 + firstReserved
    val tkOuter: Rune = 25 + firstReserved
    val tkFull: Rune = 26 + firstReserved
    val tkInto: Rune = 27 + firstReserved
    val tkDefault: Rune = 28 + firstReserved
    val tkPrimary: Rune = 29 + firstReserved
    val tkKey: Rune = 30 + firstReserved
    val tkAutoIncrement: Rune = 31 + firstReserved
    val tkValues: Rune = 32 + firstReserved
    val tkSet: Rune = 33 + firstReserved
    val tkExplain: Rune = 34 + firstReserved
    val tkOn: Rune = 35 + firstReserved
    val tkAdd: Rune = 36 + firstReserved
    val tkColumn: Rune = 37 + firstReserved
    val tkGroup: Rune = 38 + firstReserved
    val tkLimit: Rune = 39 + firstReserved
    val tkTrue: Rune = 40 + firstReserved
    val tkFalse: Rune = 41 + firstReserved
    val tkNot: Rune = 42 + firstReserved
    val tkNull: Rune = 43 + firstReserved
    val tkGe: Rune = 44 + firstReserved
    val tkLe: Rune = 45 + firstReserved
    val tkNe: Rune = 46 + firstReserved
    val tkGL: Rune = 47 + firstReserved // less or great than, 等价于 !=
    val tkNumber: Rune = 48 + firstReserved
    val tkInt: Rune = 49 + firstReserved
    val tkName: Rune = 50 + firstReserved
    val tkString: Rune = 51 + firstReserved
    val tkEOS: Rune = 52 + firstReserved

    val reservedCount: Rune = tkEOS - firstReserved + 1 // 保留关键字的数量
}

val tokens = arrayOf("describe", "show", "create", "drop", "database", "table", "index",
        "select", "insert", "update", "delete", "alter", "and", "or", "where",
        "order", "by", "asc", "desc", "from", "join", "union", "left", "right", "inner", "outer", "full",
        "into", "default", "primary", "key", "auto_increment", "values", "set",
        "explain", "on", "add", "column", "group", "limit", "true", "false",
        "not", "null",
        ">=", "<=", "!=", "<>",
        "<number>", "<int>", "<name>", "<string>", "<eof>")

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
                return "'${tokens[t - firstReserved]}'"
            }
        }
        return tokens[t - firstReserved]
    }
}