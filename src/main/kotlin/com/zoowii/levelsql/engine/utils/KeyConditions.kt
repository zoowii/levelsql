package com.zoowii.levelsql.engine.utils

import com.zoowii.levelsql.engine.index.datumsToIndexKey
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.sql.scanner.Token
import com.zoowii.levelsql.sql.scanner.TokenTypes
import java.sql.SQLException

/**
 * 比较key值的条件，用于select算子的范围筛选
 */
interface KeyCondition {
    fun match(value: ByteArray): Boolean // 是否完全匹配
    fun lessMayMatch(value: ByteArray): Boolean // 比{value}小的值是否可能匹配
    fun greaterMayMatch(value: ByteArray): Boolean // 比{value}大的值是否可能匹配
    fun acceptRange(begin: ByteArray?, end: ByteArray?): Boolean // 是否能接受[begin, end)范围内的值。begin/end为null表示无限小/无限大
    fun acceptLesserKey(): Boolean // 是否可能接受比满足条件的值更小的值（比如<或者<=条件)
    fun acceptGreaterKey(): Boolean // 是否可能接受比满足条件的值更大的值（比如>或者>=条件)

    companion object {
        fun createFromBinExpr(op: Token, rightValue: Datum): KeyCondition {
            val rightValueData = datumsToIndexKey(rightValue)
            return when(op.t) {
                '>'.toInt() -> {
                    GreatThanKeyCondition(rightValueData)
                }
                '<'.toInt() -> {
                    LessThanKeyCondition(rightValueData)
                }
                '='.toInt() -> {
                    EqualKeyCondition(rightValueData)
                }
                // TODO: 其他其中运算符的KeyCondition也要支持
                TokenTypes.tkGe -> {
                    // >=
                    throw SQLException("not supported op $op in sql filter sub query")
                }
                TokenTypes.tkLe -> {
                    // <=
                    throw SQLException("not supported op $op in sql filter sub query")
                }
                TokenTypes.tkNe -> {
                    // !=
                    throw SQLException("not supported op $op in sql filter sub query")
                }
                TokenTypes.tkGL -> {
                    // <>
                    throw SQLException("not supported op $op in sql filter sub query")
                }
                else -> {
                    throw SQLException("not supported op $op in sql filter sub query")
                }
            }
        }
    }
}

class EqualKeyCondition(val rightValue: ByteArray) : KeyCondition {
    override fun match(value: ByteArray): Boolean {
        return compareNodeKey(value, rightValue) == 0
    }

    override fun lessMayMatch(value: ByteArray): Boolean {
        return compareNodeKey(value, rightValue) > 0
    }

    override fun greaterMayMatch(value: ByteArray): Boolean {
        return compareNodeKey(value, rightValue) < 0
    }

    override fun acceptRange(begin: ByteArray?, end: ByteArray?): Boolean {
        if(begin!=null) {
            if(compareNodeKey(begin, rightValue) > 0) {
                return false
            }
        }
        if(end != null) {
            if(compareNodeKey(end, rightValue) <= 0) {
                return false
            }
        }
        return true
    }

    override fun acceptLesserKey(): Boolean {
        return false
    }

    override fun acceptGreaterKey(): Boolean {
        return false
    }
}

class LessThanKeyCondition(val rightValue: ByteArray) : KeyCondition {
    override fun match(value: ByteArray): Boolean {
        return compareNodeKey(value, rightValue) < 0
    }

    override fun lessMayMatch(value: ByteArray): Boolean {
        return true
    }

    override fun greaterMayMatch(value: ByteArray): Boolean {
        val c = compareNodeKey(value, rightValue)
        if (c < 0)
            return true
        return false
    }

    override fun acceptRange(begin: ByteArray?, end: ByteArray?): Boolean {
        if(begin!=null) {
            if(compareNodeKey(begin, rightValue) >= 0) {
                return false
            }
        }
        return true
    }

    override fun acceptLesserKey(): Boolean {
        return true
    }

    override fun acceptGreaterKey(): Boolean {
        return false
    }
}

class GreatThanKeyCondition(val rightValue: ByteArray) : KeyCondition {
    override fun match(value: ByteArray): Boolean {
        return compareNodeKey(value, rightValue) > 0
    }

    override fun lessMayMatch(value: ByteArray): Boolean {
        val c = compareNodeKey(value, rightValue)
        if(c > 0)
            return true
        return false
    }

    override fun greaterMayMatch(value: ByteArray): Boolean {
        return true
    }

    override fun acceptLesserKey(): Boolean {
        return false
    }

    override fun acceptGreaterKey(): Boolean {
        return true
    }

    override fun acceptRange(begin: ByteArray?, end: ByteArray?): Boolean {
        if(end != null) {
            if(compareNodeKey(end, rightValue) <= 0) {
                return false
            }
        }
        return true
    }
}