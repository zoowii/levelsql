package com.zoowii.levelsql.sql.ast

import com.zoowii.levelsql.sql.scanner.Token

interface Statement {
}

class ShowStatement(val line: Int, val showedInfo: String) : Statement {
    override fun toString(): String {
        return "show $showedInfo"
    }
}

class CreateDatabaseStatement(val line: Int, val dbName: String) : Statement {
    override fun toString(): String {
        return "create database $dbName"
    }
}

class SqlColumnDef(val name: String, val definition: String) {
    override fun toString(): String {
        return "$name $definition"
    }
}

class CreateTableStatement(val line: Int, val tblName: String, val columns: List<SqlColumnDef>) : Statement {
    override fun toString(): String {
        return "create table $tblName (${columns.map { it.toString() }.joinToString(", ")})"
    }
}

class CreateIndexStatement(val line: Int, val indexName: String, val tblName: String, val columns: List<String>) : Statement {
    override fun toString(): String {
        return "create index $indexName on $tblName (${columns.joinToString(", ")})"
    }
}

class DescribeTableStatement(val line: Int, val tblName: String) : Statement {
    override fun toString(): String {
        return "describle table $tblName"
    }
}

class InsertStatement(val line: Int, val tblName: String, val columns: List<String>, val rows: List<List<Token>>) : Statement {
    override fun toString(): String {
        return "insert into table $tblName (${columns.joinToString(", ")}) " +
                "values ${rows.map { "(" + it.map { it.toString() }.joinToString(", ") + ")" }}"
    }
}

interface CondExpr {

}

class TokenExpr(val token: Token) : CondExpr {
    override fun toString(): String {
        return token.toString()
    }
}

class BinOpExpr(val op: Token, val left: CondExpr, val right: CondExpr) : CondExpr {
    override fun toString(): String {
        return "$left $op $right"
    }
}

class WhereSubQuery(val cond: CondExpr) {
    override fun toString(): String {
        return cond.toString()
    }
}

class UpdateStatement(val line: Int, val tblName: String, val setItems: List<Pair<String, Token>>, val where: WhereSubQuery?) : Statement {
    override fun toString(): String {
        return "update $tblName set ${setItems.map { it.first + " = " + it.second }}" +
                (if (where != null) " where $where" else "")
    }
}

class DeleteStatement(val line: Int, val tblName: String, val where: WhereSubQuery?) : Statement {
    override fun toString(): String {
        return "delete from $tblName" + (if (where != null) " where $where" else "")
    }
}

class RefExprSubQuery(val tblName: String, val columnName: String) {
    override fun toString(): String {
        return "$tblName.$columnName"
    }
}

class OnSubQuery(val left: RefExprSubQuery, val right: RefExprSubQuery) {
    override fun toString(): String {
        return "$left = $right"
    }
}

class JoinSubQuery(val joinType: String, val target: String, val on: OnSubQuery) {
    override fun toString(): String {
        return "$joinType join $target on $on"
    }
}

class OrderBySubQuery(val column: String, val asc: Boolean) {
    override fun toString(): String {
        return "$column ${if (asc) "asc" else "desc"}"
    }
}

class GroupBySubQuery(val column: String) {
    override fun toString(): String {
        return column
    }
}

class LimitSubQuery(val offset: Long, val limit: Long) {
    override fun toString(): String {
        return "limit $offset, $limit"
    }
}

class SelectStatement(val line: Int, val selectItems: List<Token>, val froms: List<String>, val joins: List<JoinSubQuery>,
                      val where: WhereSubQuery?, val orderBys: List<OrderBySubQuery>, val groupBys: List<GroupBySubQuery>,
                      val limit: LimitSubQuery?) : Statement {
    override fun toString(): String {
        return "select ${selectItems.joinToString(", ")} from ${froms.joinToString(", ")}" +
                joins.map { it.toString() }.joinToString(", ") + " " +
                (if (where != null) where else "") + (if (orderBys.isNotEmpty()) " order by ${orderBys.joinToString(", ")}" else "") +
                (if (groupBys.isNotEmpty()) " group by ${groupBys.joinToString(", ")}" else "") + " " +
                (if (limit != null) limit else "")

    }
}

class AlterActionSubQuery(val actionType: Token, val columnName: String, val columnType: Token?) {
    override fun toString(): String {
        return "$actionType $columnName" + (if (columnType != null) " $columnType" else "")
    }
}

class AlterStatement(val line: Int, val tblName: String, val actions: List<AlterActionSubQuery>) : Statement {
    override fun toString(): String {
        return "alter table $tblName ${actions.joinToString(", ")}"
    }
}