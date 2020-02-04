package com.zoowii.levelsql.sql.ast

import com.zoowii.levelsql.sql.scanner.Token
import com.zoowii.levelsql.sql.scanner.TokenTypes
import java.lang.StringBuilder

interface Node {
}

class ShowStatement(val line: Int, val showedInfo: String) : Node {
    override fun toString(): String {
        return "show $showedInfo"
    }
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

class InsertStatement(val line: Int, val tblName: String, val columns: List<String>, val rows: List<List<Token>>) : Node {
    override fun toString(): String {
        return "insert into table $tblName (${columns.joinToString(", ")}) " +
                "values ${rows.map { "(" + it.map { it.toString() }.joinToString(", ") + ")" }}"
    }
}

class ColumnHintInfo(val tblName: String?, val column: String) {
}

interface Expr : Node {
    // 表达式中用到了哪些列
    fun usingColumns(): List<ColumnHintInfo>
    // TODO: 把Row的eval方法放入这里
}

class ExprOp(val opToken: Token) : Expr {
    override fun usingColumns(): List<ColumnHintInfo> {
        return listOf()
    }
    override fun toString(): String {
        return opToken.toString()
    }
}

class TokenExpr(val token: Token) : Expr {
    override fun usingColumns(): List<ColumnHintInfo> {
        if(token.t == TokenTypes.tkName) {
            return listOf(ColumnHintInfo(null, token.s))
        } else {
            return listOf()
        }
    }

    override fun toString(): String {
        return token.toString()
    }
}

class BinOpExpr(val op: ExprOp, val left: Expr, val right: Expr) : Expr {
    override fun usingColumns(): List<ColumnHintInfo> {
        return left.usingColumns() + right.usingColumns()
    }

    override fun toString(): String {
        val builder = StringBuilder()
        if(left.javaClass == BinOpExpr::class.java) {
            builder.append("($left) ")
        } else {
            builder.append("$left ")
        }
        builder.append(op.toString())
        if(right.javaClass == BinOpExpr::class.java) {
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

class SelectStatement(val line: Int, val selectItems: List<Token>, val froms: List<String>, val joins: List<JoinSubQuery>,
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