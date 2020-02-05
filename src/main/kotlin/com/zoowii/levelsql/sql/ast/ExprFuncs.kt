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

class SumExprFunc : ExprFunc {
    override fun invoke(chunkArgs: List<List<Datum>>): List<Datum> {
        if(chunkArgs.size!=1) {
            throw SQLException("sum func only accept 1 argument")
        }
        val chunkArg = chunkArgs[0]
        var sum: Long = 0
        for(rowItem in chunkArg) {
            if(rowItem.kind != DatumTypes.kindInt64) {
                throw SQLException("sum func only accept integer argument")
            }
            sum += rowItem.intValue!!
        }
        return listOf(Datum(DatumTypes.kindInt64, intValue = sum))
    }

    override fun isAggregateFunc(): Boolean {
        return true
    }
}

class CountExprFunc : ExprFunc {
    override fun invoke(chunkArgs: List<List<Datum>>): List<Datum> {
        val count = chunkArgs.size
        return listOf(Datum(DatumTypes.kindInt64, intValue = count.toLong()))
    }

    override fun isAggregateFunc(): Boolean {
        return true
    }
}

abstract class ArithBinExprFunc : ExprFunc {
    override fun invoke(chunkArgs: List<List<Datum>>): List<Datum> {
        if(chunkArgs.size!=2) {
            throw SQLException("arith func only accept 2 argument")
        }
        val column1 = chunkArgs[0]
        val column2 = chunkArgs[1]
        if(column1.size!=column2.size) {
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
        for(i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindInt64, intValue =  column1[i].intValue!! + column2[i].intValue!!))
        }
        return result
    }
}

class ArithMinusExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for(i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindInt64, intValue =  column1[i].intValue!! - column2[i].intValue!!))
        }
        return result
    }
}

class ArithMultiplyExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for(i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindInt64, intValue =  column1[i].intValue!! * column2[i].intValue!!))
        }
        return result
    }
}

class ArithDivExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for(i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindInt64, intValue =  column1[i].intValue!! / column2[i].intValue!!))
        }
        return result
    }
}

class ArithModExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for(i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindInt64, intValue =  column1[i].intValue!! % column2[i].intValue!!))
        }
        return result
    }
}

// >
class ArithGtExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for(i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindBool, boolValue =  column1[i].intValue!! > column2[i].intValue!!))
        }
        return result
    }
}

// <
class ArithLtExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for(i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindBool, boolValue =  column1[i].intValue!! < column2[i].intValue!!))
        }
        return result
    }
}

// >=
class ArithGeExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for(i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindBool, boolValue =  column1[i].intValue!! >= column2[i].intValue!!))
        }
        return result
    }
}

// <=
class ArithLeExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for(i in 0 until rowsCount) {
            assert(column1[i].kind == DatumTypes.kindInt64)
            assert(column2[i].kind == DatumTypes.kindInt64)
            result.add(Datum(DatumTypes.kindBool, boolValue =  column1[i].intValue!! <= column2[i].intValue!!))
        }
        return result
    }
}

// =
class EqualExprFunc : ArithBinExprFunc() {
    override fun applyArithCompute(column1: List<Datum>, column2: List<Datum>): List<Datum> {
        val rowsCount = minOf(column1.size, column2.size)
        val result = mutableListOf<Datum>()
        for(i in 0 until rowsCount) {
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
        for(i in 0 until rowsCount) {
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