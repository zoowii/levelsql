package com.zoowii.levelsql.sql.ast

import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.sql.scanner.Token
import com.zoowii.levelsql.sql.scanner.TokenTypes
import java.lang.StringBuilder
import java.sql.SQLException

interface Node {
}

class ShowStatement(val line: Int, val showedInfo: String) : Node {
    override fun toString(): String {
        return "show $showedInfo"
    }

    fun isShowDatabasesStmt(): Boolean = showedInfo.toLowerCase() == "databases"
    fun isShowTablesStmt(): Boolean = showedInfo.toLowerCase() == "tables"
    fun isShowWarningsStmt(): Boolean = showedInfo.toLowerCase() == "warnings"
}

class CreateDatabaseStatement(val line: Int, val dbName: String) : Node {
    override fun toString(): String {
        return "create database $dbName"
    }
}

class SqlColumnDef(val name: String, val definition: String) : Node {
    override fun toString(): String {
        return "$name $definition"
    }
}

class CreateTableStatement(val line: Int, val tblName: String, val columns: List<SqlColumnDef>) : Node {
    override fun toString(): String {
        return "create table $tblName (${columns.map { it.toString() }.joinToString(", ")})"
    }
}

class CreateIndexStatement(val line: Int, val indexName: String, val tblName: String, val columns: List<String>) : Node {
    override fun toString(): String {
        return "create index $indexName on $tblName (${columns.joinToString(", ")})"
    }
}

class DescribeTableStatement(val line: Int, val tblName: String) : Node {
    override fun toString(): String {
        return "describle table $tblName"
    }
}

class SetStatement(val line: Int, val paramName: String, val expr: Expr) : Node {
    override fun toString(): String {
        return "set $paramName = $expr"
    }
}

class UseStatement(val line: Int, val dbName: String) : Node {
    override fun toString(): String {
        return "use $dbName"
    }
}

class InsertStatement(val line: Int, val tblName: String, val columns: List<String>, val rows: List<List<Token>>) : Node {
    override fun toString(): String {
        return "insert into table $tblName (${columns.joinToString(", ")}) " +
                "values ${rows.map { "(" + it.map { it.toString() }.joinToString(", ") + ")" }}"
    }
}

data class ColumnHintInfo(val tblName: String?, val column: String) {
}

interface Expr : Node {
    // 表达式中用到了哪些列
    fun usingColumns(): List<ColumnHintInfo>

    // 向量化计算表达式的值,chunks是多行输入
    // @param headerNames 是本row各数据对应的header name
    fun eval(chunk: Chunk, headerNames: List<String>): List<Datum>
}

class ExprOp(val opToken: Token) : Expr {
    override fun usingColumns(): List<ColumnHintInfo> {
        return listOf()
    }

    override fun eval(chunk: Chunk, headerNames: List<String>): List<Datum> {
        throw SQLException("single op not support to eval")
    }

    override fun toString(): String {
        return opToken.toString()
    }
}

class TokenExpr(val token: Token) : Expr {
    override fun usingColumns(): List<ColumnHintInfo> {
        if (token.t == TokenTypes.tkName) {
            return listOf(ColumnHintInfo(null, token.s))
        } else {
            return listOf()
        }
    }

    override fun eval(chunk: Chunk, headerNames: List<String>): List<Datum> {
        return chunk.rows.map {
            val row = it
            when {
                token.t == TokenTypes.tkName -> row.getItem(headerNames, token.s)
                token.isLiteralValue() -> token.getLiteralDatumValue()
                else -> throw SQLException("not supported token $token in expr")
            }
        }
    }

    override fun toString(): String {
        return token.toString()
    }
}

class ColumnHintExpr(val tblName: String, val column: String) : Expr {
    override fun usingColumns(): List<ColumnHintInfo> {
        return listOf(ColumnHintInfo(tblName, column))
    }

    override fun eval(chunk: Chunk, headerNames: List<String>): List<Datum> {
        // TODO: 检查输入和tblName是否一致
        return chunk.rows.map {
            it.getItem(headerNames, column)
        }
    }

    override fun toString(): String {
        if (tblName.isBlank())
            return column
        return "$tblName.$column"
    }
}

class FuncCallExpr(val funcName: String, val args: List<Expr>) : Expr {
    companion object {
        private val availableFuncs = hashMapOf<String, ExprFunc>(
                Pair("sum", SumExprFunc()),
                Pair("count", CountExprFunc()),
                Pair("max", MaxExprFunc()),
                Pair("min", MinExprFunc())
        )
    }

    var func: ExprFunc? = null

    init {
        if (!availableFuncs.containsKey(funcName)) {
            throw SQLException("can't find function $funcName")
        }
        func = availableFuncs[funcName]
    }

    override fun usingColumns(): List<ColumnHintInfo> {
        val result = mutableSetOf<ColumnHintInfo>()
        for (item in args) {
            result.addAll(item.usingColumns())
        }
        return result.toList()
    }

    fun aggregateEval(result: MutableList<Datum>, groupIndex: Int, chunk: Chunk, headerNames: List<String>) {
        val argsValues = args.map { it.eval(chunk, headerNames) }

        if (AggregateFunc::class.java.isAssignableFrom(func!!.javaClass)) {
            val aggFunc = func!! as AggregateFunc
            aggFunc.reduce(result, groupIndex, argsValues)
        } else {
            val callResp = func!!.invoke(argsValues)
            result[groupIndex] = if (callResp.isEmpty()) Datum(DatumTypes.kindNull) else callResp[0]
        }
    }

    override fun eval(chunk: Chunk, headerNames: List<String>): List<Datum> {
        val argsValues = args.map { it.eval(chunk, headerNames) }

        if (AggregateFunc::class.java.isAssignableFrom(func!!.javaClass)) {
            val aggFunc = func!! as AggregateFunc
            val result = mutableListOf<Datum>()
            result.add(Datum(DatumTypes.kindNull)) // 目前还没实现分组，所以聚合函数的返回结构只有一行
            val groupIndex = 0
            aggFunc.reduce(result, groupIndex, argsValues)
            return result
        } else {
            return func!!.invoke(argsValues)
        }
    }

    override fun toString(): String {
        return "$funcName(${args.joinToString(", ")})"
    }
}

class BinOpExpr(val op: ExprOp, val left: Expr, val right: Expr) : Expr {
    override fun usingColumns(): List<ColumnHintInfo> {
        return left.usingColumns() + right.usingColumns()
    }

    override fun eval(chunk: Chunk, headerNames: List<String>): List<Datum> {
        val leftValues = left.eval(chunk, headerNames)
        val rightValues = right.eval(chunk, headerNames)
        if (!arithFuncs.containsKey(op.opToken.t)) {
            throw SQLException("not supported op $op in expr")
        }
        val func = arithFuncs[op.opToken.t]!!
        return func.invoke(listOf(leftValues, rightValues))
    }

    override fun toString(): String {
        val builder = StringBuilder()
        if (left.javaClass == BinOpExpr::class.java) {
            builder.append("($left) ")
        } else {
            builder.append("$left ")
        }
        builder.append(op.toString())
        builder.append(" ")
        if (right.javaClass == BinOpExpr::class.java) {
            builder.append("($right)")
        } else {
            builder.append("$right")
        }
        return builder.toString()
    }
}

class WhereSubQuery(val cond: Expr) : Node {
    override fun toString(): String {
        return "where $cond"
    }
}

class UpdateStatement(val line: Int, val tblName: String, val setItems: List<Pair<String, Token>>, val where: WhereSubQuery?) : Node {
    override fun toString(): String {
        return "update $tblName set ${setItems.map { it.first + " = " + it.second }}" +
                (if (where != null) " where $where" else "")
    }
}

class DeleteStatement(val line: Int, val tblName: String, val where: WhereSubQuery?) : Node {
    override fun toString(): String {
        return "delete from $tblName" + (if (where != null) " where $where" else "")
    }
}

class RefExprSubQuery(val tblName: String, val columnName: String) : Node {
    override fun toString(): String {
        return "$tblName.$columnName"
    }
}

class OnSubQuery(val left: RefExprSubQuery, val right: RefExprSubQuery) : Node {
    override fun toString(): String {
        return "$left = $right"
    }
}

class JoinSubQuery(val joinType: String, val target: String, val on: OnSubQuery) : Node {
    override fun toString(): String {
        return "$joinType join $target on $on"
    }
}

class OrderBySubQuery(val column: String, val asc: Boolean) : Node {
    override fun toString(): String {
        return "$column ${if (asc) "asc" else "desc"}"
    }
}

class GroupBySubQuery(val column: String) : Node {
    override fun toString(): String {
        return column
    }
}

class LimitSubQuery(val offset: Long, val limit: Long) : Node {
    override fun toString(): String {
        return "limit $offset, $limit"
    }
}

class SelectStatement(val line: Int, val selectItems: List<Expr>, val froms: List<String>, val joins: List<JoinSubQuery>,
                      val where: WhereSubQuery?, val orderBys: List<OrderBySubQuery>, val groupBys: List<GroupBySubQuery>,
                      val limit: LimitSubQuery?) : Node {
    override fun toString(): String {
        return "select ${selectItems.joinToString(", ")} from ${froms.joinToString(", ")}" +
                joins.map { it.toString() }.joinToString(", ") + " " +
                (where ?: "") + (if (orderBys.isNotEmpty()) " order by ${orderBys.joinToString(", ")}" else "") +
                (if (groupBys.isNotEmpty()) " group by ${groupBys.joinToString(", ")}" else "") + " " +
                (limit ?: "")

    }
}

class AlterActionSubQuery(val actionType: Token, val columnName: String, val columnType: Token?) : Node {
    override fun toString(): String {
        return "$actionType $columnName" + (if (columnType != null) " $columnType" else "")
    }
}

class AlterStatement(val line: Int, val tblName: String, val actions: List<AlterActionSubQuery>) : Node {
    override fun toString(): String {
        return "alter table $tblName ${actions.joinToString(", ")}"
    }
}