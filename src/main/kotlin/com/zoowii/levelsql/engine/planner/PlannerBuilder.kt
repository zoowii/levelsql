package com.zoowii.levelsql.engine.planner

import com.zoowii.levelsql.*
import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.sql.ast.*
import java.sql.SQLException

// 把SQL AST转成planner树
object PlannerBuilder {
    fun sqlNodeToPlanner(session: DbSession, stmt: Statement): LogicalPlanner {
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
                val projection = ProjectionPlanner(session, stmt.selectItems.map { it.toString() })
                var currentLevel: LogicalPlanner = projection
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

    // 对逻辑计划进行优化
    fun optimiseLogicalPlanner(planner: LogicalPlanner): LogicalPlanner {
        // TODO
        // TODO: 对应filter条件能用索引的，用IndexSelectPlanner修改下层的select planner
        return planner
    }
}