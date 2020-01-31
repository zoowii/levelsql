package com.zoowii.levelsql.engine.planner

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.sql.ast.CondExpr
import com.zoowii.levelsql.sql.ast.JoinSubQuery

// 从table中检索数据的planner
class SelectPlanner(private val sess: DbSession, val tblName: String) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "select $tblName${childrenToString()}"
    }
}

// 从索引中检索数据的planner
class IndexSelectPlanner(private val sess: DbSession, val tblName: String, val indexName: String,
                         val indexColumns: List<String>) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "index select $tblName by index $indexName (${indexColumns.joinToString(", ")})${childrenToString()}"
    }
}

// 聚合操作的planner
class AggregatePlanner(private val sess: DbSession, val funcName: String, val column: String) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "aggregate $funcName($column)${childrenToString()}"
    }
}

// 从输入中投影出部分列的planner
class ProjectionPlanner(private val sess: DbSession, val columns: List<String>) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "projection ${columns.joinToString(", ")}${childrenToString()}"
    }
}

// join table操作的planner
class JoinPlanner(private val sess: DbSession, val joinConditions: List<JoinSubQuery>) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "${joinConditions.joinToString(", ")}${childrenToString()}"
    }
}

// 按条件过滤数据的planner
class FilterPlanner(private val sess: DbSession, val cond: CondExpr) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "filter by $cond${childrenToString()}"
    }
}

// 按条件过滤的planner
class OrderByPlanner(private val sess: DbSession, val column: String, val asc: Boolean) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "order by $column ${if(asc) "asc" else "desc"}${childrenToString()}"
    }
}

// 对输入数据进行分组的planner
class GroupByPlanner(private val sess: DbSession, val column: String) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "group by $column${childrenToString()}"
    }
}

// 限制查询数据行数和偏移量的planner
class LimitPlanner(private val sess: DbSession, val offset: Long, val limit: Long) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "limit $offset, $limit${childrenToString()}"
    }
}

// 笛卡尔积的planner
class ProductPlanner(private val sess: DbSession) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "product${childrenToString()}"
    }
}