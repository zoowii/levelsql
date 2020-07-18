package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.sql.ast.ColumnHintExpr
import com.zoowii.levelsql.sql.ast.Expr
import com.zoowii.levelsql.sql.ast.FuncCallExpr
import com.zoowii.levelsql.sql.ast.TokenExpr
import java.util.concurrent.Future


// 从输入中投影出部分列的planner
class ProjectionPlanner(private val sess: IDbSession, val columns: List<Expr>) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "projection ${columns.joinToString(", ")}${childrenToString()}"
    }

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {

    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask,
                                        childrenFetchTasks: List<FetchTask>) {
        if (fetchTask.isEnd()) {
            return
        }
        val (mergedChunk, hasSourceEnd, error) = mergeChildrenChunks(childrenFetchTasks)
        if (error != null) {
            fetchTask.submitError(error)
            return
        }
        if (mergedChunk.rows.isEmpty() && hasSourceEnd) {
            fetchTask.submitSourceEnd()
            return
        }
        assert(children.isNotEmpty())
        val childrenOutputNames = children[0].getOutputNames()
        val outputExprs = getOutputExprs()


        val projectionRows = mutableListOf<Row>()

        // 对输入数据(来自children的输出)做projection
        // 向量化计算得到各列的输出
        try {
            val projectionColumns = outputExprs.map {
                it.eval(mergedChunk, childrenOutputNames)
            }
            // 把列式输出转成行式输出
            val rowsCount = projectionColumns.map { it.size }.min() ?: 0
            for (i in 0 until rowsCount) {
                val row = Row()
                row.data = projectionColumns.map { it[i] }
                projectionRows.add(row)
            }
        } catch (e: Exception) {
            fetchTask.submitError(e.message!!)
            return
        }
        fetchTask.submitChunk(Chunk().replaceRows(projectionRows))
    }

    fun hasAggregateFunc(): Boolean {
        return columns.any {
            it.javaClass == FuncCallExpr::class.java && (it as FuncCallExpr).func!!.isAggregateFunc()
        }
    }

    private fun getOutputExprs(): List<Expr> {
        // columns中某些项可能是复杂表达式，这种情况需要eval。也有的情况是聚合函数调用，需要对聚合函数中用到的各列eval
        val outputExprs = mutableListOf<Expr>()
        val inOutputColumns = mutableSetOf<String>()
        for (column in columns) {
            if (column.javaClass == FuncCallExpr::class.java) {
                column as FuncCallExpr
                // column中如果用到outputExprs中已经有的单符号列，则直接复用
                column.usingColumns().map {
                    // 先不管是哪个table
                    if (inOutputColumns.contains(it.column)) {
                        true
                    } else {
                        if (it.tblName != null) {
                            outputExprs.add(ColumnHintExpr(it.tblName, it.column))
                        } else {
                            outputExprs.add(ColumnHintExpr("", it.column))
                        }
                        inOutputColumns.add(it.column)
                    }
                }
                continue
            }
            // 如果不包含*，则output names是选择的列，如果包含*，则是选择的列 + children[0]（如果children非空）的各列
            if (column.javaClass == TokenExpr::class.java && (column as TokenExpr).token.t == '*'.toInt()) {
                assert(children.isNotEmpty())
                outputExprs.addAll(children[0].getOutputNames().map {
                    ColumnHintExpr("", it)
                })
                continue
            }
            outputExprs.add(column)
        }
        return outputExprs
    }

    override fun setSelfOutputNames() {
        val outputNames = getOutputExprs().map { it.toString() }
        setOutputNames(outputNames)
    }
}