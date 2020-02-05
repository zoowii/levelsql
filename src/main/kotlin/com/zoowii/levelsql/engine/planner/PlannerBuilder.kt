package com.zoowii.levelsql.engine.planner

import com.zoowii.levelsql.*
import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.sql.ast.*
import java.sql.SQLException

// 把SQL AST转成planner树
object PlannerBuilder {
    fun sqlNodeToPlanner(session: DbSession, stmt: Node): LogicalPlanner {
        when(stmt.javaClass) {
            SelectStatement::class.java -> {
                /**
                 * select语句对应logical planner为(下层planner的输出是上一层planner的输入)
                 *                      aggregate(比如count(1), max(age)等)
                 *                          |
                 *                      projection
                 *                          |
                 *                        limit
                 *                          |
                 *                       order by (多层，每层order by的输入是上一层order by的输出)
                 *                          |
                 *                       group by (多层，每层group by的输入是上一层group by的输出)
                 *                          |
                 *                        filter
                 *                          |
                 *                         join
                 *                    |              |
                 *        product(对输入做笛卡尔积)   filter(根据join on条件以及join type来过滤)
                 *                     |                        |
                 *    select from tables(from A1, A2)        select from joined-tables
                 */
                stmt as SelectStatement
                // TODO: 各planner的实例需要设置输出的各列名称

                // 目前没有聚合操作，顶层直接就是projection
                val projection = ProjectionPlanner(session, stmt.selectItems)
                var currentLevel: LogicalPlanner = projection
                var topLevel = projection
                // TODO: 如果projection中有聚合函数（而不是普通单行计算函数），需要在projection之上增加聚合算子，并且projection计算的输出遇到聚合函数时要输出聚合函数要求的列

                if(stmt.limit!=null) {
                    val limitPlanner = LimitPlanner(session, stmt.limit.offset, stmt.limit.limit)
                    currentLevel.addChild(limitPlanner)
                    currentLevel = limitPlanner
                }
                if(stmt.orderBys.isNotEmpty()) {
                    // sql中最前的order by子句对应的planner在树的最底层，需要优先对数据源处理
                    for(orderBy in stmt.orderBys.asReversed()) {
                        val orderByPlanner = OrderByPlanner(session, orderBy.column, orderBy.asc)
                        currentLevel.addChild(orderByPlanner)
                        currentLevel = orderByPlanner
                    }
                }
                if(stmt.groupBys.isNotEmpty()) {
                    // sql中最前的group by子句对应的planner在树的最底层，需要优先对数据源处理
                    for(groupBy in stmt.groupBys.asReversed()) {
                        val orderByPlanner = GroupByPlanner(session, groupBy.column)
                        currentLevel.addChild(orderByPlanner)
                        currentLevel = orderByPlanner
                    }
                }
                if(stmt.where!=null) {
                    val filterPlanner = FilterPlanner(session, stmt.where.cond)
                    currentLevel.addChild(filterPlanner)
                    currentLevel = filterPlanner
                }
                if(stmt.froms.isEmpty()) {
                    throw SQLException("invalid select sql, no from sub query")
                }
                val productPlanner = ProductPlanner(session)
                // productPlanner 下级是从各表中取数据
                for(tbl in stmt.froms) {
                    productPlanner.addChild(SelectPlanner(session, tbl))
                }

                if(stmt.joins.isNotEmpty()) {
                    val joinPlanner = JoinPlanner(session, stmt.joins)
                    currentLevel.addChild(joinPlanner)
                    currentLevel = joinPlanner
                    // 给joinPlanner下面加上from后各表以及各被join表的fromPlanner
                    for(join in stmt.joins) {
                        productPlanner.addChild(SelectPlanner(session, join.target))
                    }

                    joinPlanner.addChild(productPlanner)
                } else {
                    // 直接下级是product planner
                    if(stmt.froms.size==1) {
                        val fromPlanner = SelectPlanner(session, stmt.froms[0])
                        currentLevel.addChild(fromPlanner)
                        currentLevel = fromPlanner
                    } else {
                        currentLevel.addChild(productPlanner)
                        currentLevel = productPlanner
                    }
                }

                projection.setTreeOutputNames()

                return projection
            }
            InsertStatement::class.java -> {
                stmt as InsertStatement
                return InsertPlanner(session, stmt.tblName, stmt.columns, stmt.rows)
            }
            CreateDatabaseStatement::class.java -> {
                stmt as CreateDatabaseStatement
                return CreateDatabasePlanner(session, stmt.dbName)
            }
            CreateTableStatement::class.java -> {
                stmt as CreateTableStatement
                // TODO: 暂时只用id这个字段作为主键
                val primaryKeyColumn = stmt.columns.firstOrNull { it.name == "id" }
                        ?: throw SQLException("no primary key provided")
                val columnDefinitions = stmt.columns.map {
                    val nullable = true // 目前默认就用nullable
                    // TODO: 需要解析出具体的类型定义
                    val definitionLower = it.definition.toLowerCase()
                    val columnType = when {
                        definitionLower == "int" -> IntColumnType()
                        definitionLower == "string" -> TextColumnType()
                        definitionLower == "text" -> TextColumnType()
                        definitionLower.startsWith("varchar") -> VarCharColumnType(100)
                        definitionLower == "bool" -> BoolColumnType()
                        else -> throw SQLException("unknown column definition ${it.definition}")
                    }
                    TableColumnDefinition(it.name, columnType, nullable)
                }
                return CreateTablePlanner(session, stmt.tblName, columnDefinitions, primaryKeyColumn.name)
            }
            CreateIndexStatement::class.java -> {
                stmt as CreateIndexStatement
                val unique = false // 当前索引都按非unique索引处理
                return CreateIndexPlanner(session, stmt.indexName, stmt.tblName, stmt.columns, unique)
            }
            // TODO: 其他SQL AST节点类型
            else -> {
                throw SQLException("not supported sql node type ${stmt.javaClass} to planner")
            }
        }
    }

    fun afterPlannerOptimised(session: DbSession, planner: LogicalPlanner) {
        planner.setTreeOutputNames()
    }

    // 对逻辑计划进行优化
    fun optimiseLogicalPlanner(session: DbSession, planner: LogicalPlanner): LogicalPlanner {
        // TODO
        // TODO: 对应filter条件能用索引的，用IndexSelectPlanner修改下层的select planner

        val onlyOptimiseChildren = { ->
            for(i in 0 until planner.children.size) {
                val child = planner.children[i]
                planner.setChild(i, optimiseLogicalPlanner(session, child))
            }
            planner
        }

        when(planner.javaClass) {
            FilterPlanner::class.java -> {
                planner as FilterPlanner
                // 获取filter planner用到的所有table.column
                val filterColumns = planner.cond.usingColumns()
                val db = session.db ?: throw SQLException("please use one database first")
                if(planner.children.size==1) {
                    // 为简化实现，暂时只对只检索一个表的情况做索引优化
                    for (i in 0 until planner.children.size) {
                        val child = planner.children[i]
                        // 判断filter下方是否是select表，并且filter的字段能使用到select的表中的索引，如果是，则可以优化为索引访问
                        if (child.javaClass == SelectPlanner::class.java) {
                            child as SelectPlanner
                            val table = db.openTable(child.tblName)
                            val index = table.findIndexByColumns(filterColumns) ?: continue
                            // 为简化实现，暂时只先处理使用主键索引的情况
                            // TODO: 如果用了二级索引，输出的列不够祖先planner使用的（查看最顶层planner的outputNames），则需要做回表
                            if(!index.primary)
                                continue
                            val sortAsc = true // TODO: 需要从上级planner中搜集到各列要求索引检索的排序顺序, 暂时索引检索只用增序
                            val indexPlanner = IndexSelectPlanner(session, table.tblName, index.indexName, sortAsc, planner.cond)
                            indexPlanner.children = child.children
                            planner.setChild(i, indexPlanner)
                        }
                    }
                }
            }
            else -> {
                return onlyOptimiseChildren()
            }
        }
        return onlyOptimiseChildren()
    }
}