package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.index.IndexNodeValue
import com.zoowii.levelsql.engine.index.datumsToIndexKey
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.*
import com.zoowii.levelsql.sql.ast.*
import com.zoowii.levelsql.sql.scanner.TokenTypes
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkAnd
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkGL
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkGe
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkLe
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkName
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkNe
import java.sql.SQLException
import java.util.concurrent.Future


// 从索引中检索数据的planner
class IndexSelectPlanner(private val sess: DbSession, val tblName: String, val indexName: String,
                         val asc: Boolean, val filterCondExpr: Expr) : LogicalPlanner(sess) {
    private val log = logger()

    override fun toString(): String {
        return "index select $tblName by index $indexName ${if (asc) "asc" else "desc"} $filterCondExpr ${childrenToString()}"
    }

    private var seekedPos: IndexNodeValue? = null // 上次已经检索到的记录的位置

    private var sourceEnd = false

    // 从filterExpr中找到若干个可能用and连接的等于比较式，如果有or，不做过滤，如果有非等于比较，忽略
    private fun getCompareConditionsFromFilterExpr(filterExpr: Expr): List<BinOpExpr> {
        when (filterExpr.javaClass) {
            BinOpExpr::class.java -> {
                filterExpr as BinOpExpr
                when (filterExpr.op.opToken.t) {
                    tkAnd -> {
                        return getCompareConditionsFromFilterExpr(filterExpr.left) + getCompareConditionsFromFilterExpr(filterExpr.right)
                    }
                    '='.toInt(), '>'.toInt(), '<'.toInt(), tkGL, tkGe, tkNe, tkLe -> {
                        // 为简化实现，用于索引查找的条件，只能一侧是columnHint或者TokenExpr+symbol，另外一侧是字面量
                        val left = filterExpr.left
                        val right = filterExpr.right
                        if (ColumnHintExpr::class.java.isAssignableFrom(left.javaClass)
                                || (left.javaClass == TokenExpr::class.java && (left as TokenExpr).token.t == tkName)) {
                            // 如果左侧是columnHint或者symbol tokenExpr，则右侧是字面量的时候才能用来索引查询
                            if (!right.isLiteralExpr()) {
                                return listOf()
                            }
                            return listOf(BinOpExpr(filterExpr.op, left, right))
                        }
                        if (ColumnHintExpr::class.java.isAssignableFrom(right.javaClass)
                                || (right.javaClass == TokenExpr::class.java && (right as TokenExpr).token.t == tkName)) {
                            // 如果右侧是columnHint或者symbol tokenExpr，则左侧是字面量的时候才能用来索引查询
                            if (!left.isLiteralExpr()) {
                                return listOf()
                            }
                            return listOf(BinOpExpr(filterExpr.op, right, left)) // 保持返回结果左侧是列，右侧是值
                        }
                        return listOf()
                    }
                    else -> return listOf()
                }
            }
            else -> return listOf()
        }
    }

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if (sess.db == null) {
            throw SQLException("database not opened. need use one-database")
        }
        if (sourceEnd) {
            fetchTask.submitSourceEnd()
            return
        }
        val table = sess.db!!.openTable(tblName)
        val index = table.openIndex(sess, indexName) ?: throw SQLException("can't find index $indexName")
        // 从filterCondExpr中构造在index的tree种seek用的KeyCondition，然后在index中搜索
        var keyCondition: KeyCondition? = null
        // 为简化实现，目前只接受过滤条件是一项的 column op literalValue 的二元条件表达式 或者 a=xxx and b = xxx 这类 and 以及等于表达式且可以用二级索引的条件
        val compareFilterExprs = getCompareConditionsFromFilterExpr(filterCondExpr)

        // 在equalFilterExprs中找到满足这个索引的若干个等于表达式，用于构造keyCondition
        val inIndexFilterExprs = mutableMapOf<String, Datum>() // map of columnName => rightValue
        for (itemExpr in compareFilterExprs) {
            if (itemExpr.op.opToken.t != '='.toInt()) {
                continue
            }
            val left = itemExpr.left
            val right = itemExpr.right as TokenExpr
            if (ColumnHintExpr::class.java.isAssignableFrom(left.javaClass)) {
                left as ColumnHintExpr
                if (left.tblName.isNotEmpty() && left.tblName != tblName) {
                    continue // 非本索引的表的字段
                }
                if (!index.columns.contains(left.column)) {
                    continue // 非本索引的字段
                }
                if (inIndexFilterExprs.containsKey(left.column)) {
                    continue // 条件表达式中多个这个字段的等于表达式，只取第一个用来过滤
                }
                inIndexFilterExprs[left.column] = right.token.getLiteralDatumValue()
            } else if (TokenExpr::class.java.isAssignableFrom(left.javaClass)) {
                left as TokenExpr
                val leftColumnName = left.token.s
                if (!index.columns.contains(leftColumnName)) {
                    continue // 非本索引的字段
                }
                if (inIndexFilterExprs.containsKey(leftColumnName)) {
                    continue // 条件表达式中多个这个字段的等于表达式，只取第一个用来过滤
                }
                inIndexFilterExprs[leftColumnName] = right.token.getLiteralDatumValue()
            }
        }
        // 把inIndexFilterExprs按索引的各字段顺序重新排序. 如果某些字段没有提供值，用本类型的0值填充并且keyCondition从equal condition变成 greatThanOrEqual condition
        val indexColumnValues = mutableListOf<Datum>()
        var hasNotFilledColumns = false
        for (colName in index.columns) {
            if (inIndexFilterExprs.containsKey(colName)) {
                indexColumnValues.add(inIndexFilterExprs[colName]!!)
            } else {
                // 条件表达式没提供索引这个字段的值，用0值填充
                hasNotFilledColumns = true
                // 获取索引这个字段的类型，以及获得这个类型的默认Datum值
                val columnInfo = table.columns.first { it.name == colName }
                val defaultDatum = columnInfo.columnType.defaultDatum()
                indexColumnValues.add(defaultDatum)
            }
        }
        val indexKeyValue = datumsToIndexKey(indexColumnValues)
        if (index.columns.size == 1 && compareFilterExprs.size == 1) {
            // 索引只有一列并且提供了条件时，允许 = 外的其他操作符
            val filterRightValue = (compareFilterExprs[0].right as TokenExpr).token.getLiteralDatumValue()
            keyCondition = KeyCondition.createFromBinExpr(compareFilterExprs[0].op.opToken, filterRightValue)
        } else {
            if (hasNotFilledColumns) {
                keyCondition = GreatThanKeyCondition(indexKeyValue) // TODO: 改成 GreatEqualKeyCondition
            } else {
                keyCondition = EqualKeyCondition(indexKeyValue)
            }
        }

        if (seekedPos == null) {
            seekedPos = index.tree.seekByCondition(keyCondition)
        } else {
            if (asc) {
                seekedPos = index.tree.nextRecordPosition(seekedPos!!)
            } else {
                seekedPos = index.tree.prevRecordPosition(seekedPos!!)
            }
            if (seekedPos != null) {
                // 需要检查这条记录是否满足要求
                val record = seekedPos!!.leafRecord()
                if (!keyCondition.match(record.key)) {
                    seekedPos = null
                }
            }
        }
        if (seekedPos == null) {
            // seeked to end
            log.debug("table $tblName select end")
            sourceEnd = true
            fetchTask.submitSourceEnd()
            return
        }
        val record = seekedPos!!.leafRecord()
        val row = Row().fromBytes(ByteArrayStream(record.value))
        log.debug("index select planner fetched row: ${row}")
        fetchTask.submitChunk(Chunk().replaceRows(listOf(row)))
    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask,
                                        childrenFetchTasks: List<FetchTask>) {
        simplePassChildrenTasks(fetchTask, childrenFetchTasks)
    }

    override fun setSelfOutputNames() {
        if (sess.db == null) {
            return
        }
        val table = sess.db!!.openTable(tblName)
        val index = table.openIndex(sess, indexName) ?: throw SQLException("can't find index $indexName")
        if (index.primary) {
            setOutputNames(table.columns.map { it.name })
        } else {
            setOutputNames(index.columns)
        }
    }
}
