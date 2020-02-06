package com.zoowii.levelsql.sql.ast

import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.sql.scanner.TokenTypes
import java.sql.SQLException

// 一些内置聚合和普通SQL行内函数

// SQL表达式中支持的函数的接口
interface ExprFunc {
    // 参数是多行输入对应的函数的各参数
    fun invoke(chunkArgs: List<List<Datum>>): List<Datum>

    fun isAggregateFunc(): Boolean
}

abstract class AggregateFunc : ExprFunc {
    override fun isAggregateFunc(): Boolean {
        return true
    }

    // result是各组的累积结果，长度和分组数一致. groupIndex表示这是第几个分组, chunkArgs中是本次分组中的数据
    abstract fun reduce(result: MutableList<Datum>, groupIndex: Int, chunkArgs: List<List<Datum>>)
}

class SumExprFunc : AggregateFunc() {
    override fun reduce(result: MutableList<Datum>, groupIndex: Int, chunkArgs: List<List<Datum>>) {
        if (chunkArgs.size != 1) {
            throw SQLException("sum func only accept 1 argument")
        }
        val chunkArg = chunkArgs[0]
        var sum: Long = 0
        for (rowItem in chunkArg) {
            if (rowItem.kind != DatumTypes.kindInt64) {
                throw SQLException("sum func only accept integer argument")
            }
            sum += rowItem.intValue!!
        }
        if (result[groupIndex].kind == DatumTypes.kindNull) {
            result[groupIndex] = Datum(DatumTypes.kindInt64, intValue = 0)
        }
        result[groupIndex].intValue = result[groupIndex].intValue!! + sum
        return
    }

    override fun invoke(chunkArgs: List<List<Datum>>): List<Datum> {
        return listOf()
    }

}

class CountExprFunc : AggregateFunc() {
    override fun reduce(result: MutableList<Datum>, groupIndex: Int, chunkArgs: List<List<Datum>>) {
        if (chunkArgs.size != 1) {
            throw SQLException("count func only accept 1 argument")
        }
        val chunkArg = chunkArgs[0]
        val count = chunkArg.size.toLong()
        if (result[groupIndex].kind == DatumTypes.kindNull) {
            result[groupIndex] = Datum(DatumTypes.kindInt64, intValue = 0)
        }
        result[groupIndex].intValue = result[groupIndex].intValue!! + count
        return
    }

    override fun invoke(chunkArgs: List<List<Datum>>): List<Datum> {
        return listOf()
    }
}

class MaxExprFunc : AggregateFunc() {
    override fun reduce(result: MutableList<Datum>, groupIndex: Int, chunkArgs: List<List<Datum>>) {
        if (chunkArgs.size != 1) {
            throw SQLException("max func only accept 1 argument")
        }
        val chunkArg = chunkArgs[0]
        // 目前只能对整数列求max
        val maxValue = chunkArg.map { it.intValue!! }.max()
        if (result[groupIndex].kind == DatumTypes.kindNull) {
            result[groupIndex] = Datum(DatumTypes.kindInt64, intValue = 0)
        }
        if (maxValue == null) {
            result[groupIndex].intValue = result[groupIndex].intValue!!
        } else {
            result[groupIndex].intValue = maxOf(result[groupIndex].intValue!!, maxValue)
        }
        return
    }

    override fun invoke(chunkArgs: List<List<Datum>>): List<Datum> {
        return listOf()
    }
}

class MinExprFunc : AggregateFunc() {
    override fun reduce(result: MutableList<Datum>, groupIndex: Int, chunkArgs: List<List<Datum>>) {
        if (chunkArgs.size != 1) {
            throw SQLException("min func only accept 1 argument")
        }
        val chunkArg = chunkArgs[0]
        // 目前只能对整数列求max
        val minValue = chunkArg.map { it.intValue!! }.min()
        if (result[groupIndex].kind == DatumTypes.kindNull) {
            result[groupIndex] = Datum(DatumTypes.kindInt64, intValue = 0)
        }
        if (minValue == null) {
            result[groupIndex].intValue = result[groupIndex].intValue!!
        } else {
            result[groupIndex].intValue = maxOf(result[groupIndex].intValue!!, minValue)
        }
        return
    }

    override fun invoke(chunkArgs: List<List<Datum>>): List<Datum> {
        return listOf()
    }
}

abstract class ArithBinExprFunc : ExprFunc {
    override fun invoke(chunkArgs: List<List<Datum>>): List<Datum> {
        if (chunkArgs.size != 2) {
            throw SQLException("arith func only accept 2 argument")
        }
        val column1 = chunkArgs[0]
        val column2 = chunkArgs[1]
        if (column1.size != column2.size) {
            throw SQLException("arith func only accept 2 argument(invalid rows count)")
        }
        return applyArithCompute(column1, column2)
    }

    protected abstract fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum>

    override fun isAggregateFunc(): Boolean {
        return false
    }
}

class ArithAddExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for (i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindInt64, intValue = column1[i].intValue!! + column2[i].intValue!!))
        }
        return result
    }
}

class ArithMinusExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for (i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindInt64, intValue = column1[i].intValue!! - column2[i].intValue!!))
        }
        return result
    }
}

class ArithMultiplyExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for (i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindInt64, intValue = column1[i].intValue!! * column2[i].intValue!!))
        }
        return result
    }
}

class ArithDivExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for (i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindInt64, intValue = column1[i].intValue!! / column2[i].intValue!!))
        }
        return result
    }
}

class ArithModExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for (i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindInt64, intValue = column1[i].intValue!! % column2[i].intValue!!))
        }
        return result
    }
}

// >
class ArithGtExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for (i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindBool, boolValue = column1[i].intValue!! > column2[i].intValue!!))
        }
        return result
    }
}

// <
class ArithLtExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for (i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindBool, boolValue = column1[i].intValue!! < column2[i].intValue!!))
        }
        return result
    }
}

// >=
class ArithGeExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for (i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindBool, boolValue = column1[i].intValue!! >= column2[i].intValue!!))
        }
        return result
    }
}

// <=
class ArithLeExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for (i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindBool, boolValue = column1[i].intValue!! <= column2[i].intValue!!))
        }
        return result
    }
}

// =
class EqualExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for (i in 0 until rowsCount) {
            assert(column1[i].kind == column2[i].kind)
            result.add(Datum(DatumTypes.kindBool, boolValue = column1[i].toString() == column2[i].toString()))
        }
        return result
    }
}

// !=
class NotEqualExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for (i in 0 until rowsCount) {
            assert(column1[i].kind == column2[i].kind)
            result.add(Datum(DatumTypes.kindBool, boolValue = column1[i].toString() != column2[i].toString()))
        }
        return result
    }
}


// 内置的各运算符函数
val arithFuncs = hashMapOf(
        Pair('+'.toInt(), ArithAddExprFunc()),
        Pair('-'.toInt(), ArithMinusExprFunc()),
        Pair('*'.toInt(), ArithMultiplyExprFunc()),
        Pair('/'.toInt(), ArithDivExprFunc()),
        Pair('%'.toInt(), ArithModExprFunc()),
        Pair('>'.toInt(), ArithGtExprFunc()),
        Pair('<'.toInt(), ArithLtExprFunc()),
        Pair(TokenTypes.tkGe, ArithGeExprFunc()),
        Pair(TokenTypes.tkLe, ArithLeExprFunc()),
        Pair('='.toInt(), EqualExprFunc()),
        Pair(TokenTypes.tkNe, NotEqualExprFunc()),
        Pair(TokenTypes.tkGL, NotEqualExprFunc())
)