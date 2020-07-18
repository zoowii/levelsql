package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.sql.ast.AggregateFunc
import com.zoowii.levelsql.sql.ast.Expr
import com.zoowii.levelsql.sql.ast.FuncCallExpr
import java.util.concurrent.Future


// 聚合操作的planner, @param columns 可能是group by中的列，也可能是聚合函数调用比如 count(1), sum(age)等
class AggregatePlanner(private val sess: IDbSession, val columns: List<Expr>) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "aggregate ${columns.joinToString(", ")}${childrenToString()}"
    }

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {

    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {

    }

    private var aggregatedDone = false
    // 累计的分组聚合结果, columnIndex => 各分组暂时的聚合结果
    private var tmpAggregateColumnValues: MutableList<MutableList<Datum>> = mutableListOf()

    override fun afterChildrenTasksDone(fetchTask: FetchTask,
                                        childrenFetchTasks: List<FetchTask>) {
        if (fetchTask.isEnd()) {
            return
        }
        if (aggregatedDone) {
            fetchTask.submitSourceEnd()
            return
        }
        val groupIndex = 0
        // TODO: 实现分组后，聚合算子的输入要知道是哪个分组(方便并行执行不同分组的数据)
        // 目前因为还没有实现分组，所以多次输入都是同一个分组，要把结果记录下来等待累计值

        // 聚合函数要得到部分结果的时候可以先做部分运算，不是每次得到一个结果最终输出多个结果，也不是等到所有都输出完成再给结果. 聚合前需要先分组
        val (mergedChunk, hasSourceEnd, error) = mergeChildrenChunks(childrenFetchTasks)
        if (error != null) {
            fetchTask.submitError(error)
            return
        }
        if (mergedChunk.rows.isEmpty() && hasSourceEnd) {
            aggregatedDone = true
            // 把累计的分组聚合结果输出
            // 把列式输出转成行式输出
            val resultRows = mutableListOf<Row>()
            val rowsCount = tmpAggregateColumnValues.map { it.size }.min() ?: 0
            for (i in 0 until rowsCount) {
                val row = Row()
                row.data = tmpAggregateColumnValues.map { it[i] }
                resultRows.add(row)
            }
            fetchTask.submitChunk(Chunk().replaceRows(resultRows))
            return
        }
        assert(children.isNotEmpty())
        val childrenOutputNames = children[0].getOutputNames()

        // 要求输入是分组后的数据
        // 对columns各表达式用输入数据(来自children的输出)做eval
        // 向量化计算得到各列的输出
        try {

            // 先给tmpAggregateColumnValues 分配足够的空间
            while (tmpAggregateColumnValues.size < columns.size) {
                val tmpColumnValues = mutableListOf<Datum>()
                tmpAggregateColumnValues.add(tmpColumnValues)
            }
            for (tmpColumnValues in tmpAggregateColumnValues) {
                while (tmpColumnValues.size <= groupIndex) {
                    tmpColumnValues.add(Datum(DatumTypes.kindNull))
                }
            }
            columns.mapIndexed { index, column ->
                when {
                    FuncCallExpr::class.java.isAssignableFrom(column.javaClass)
                            && AggregateFunc::class.java.isAssignableFrom((column as FuncCallExpr).func!!.javaClass) -> {
                        // 是聚合函数调用
                        column as FuncCallExpr
                        column.aggregateEval(tmpAggregateColumnValues[index], groupIndex, mergedChunk, childrenOutputNames)
                    }
                    else -> {
                        val columnValues = column.eval(mergedChunk, childrenOutputNames)
                        tmpAggregateColumnValues[index][groupIndex] = if (columnValues.isEmpty())
                            Datum(DatumTypes.kindNull)
                        else
                            columnValues[0]
                    }
                }
            }
        } catch (e: Exception) {
            fetchTask.submitError(e.message!!)
            return
        }
        fetchTask.submitChunk(Chunk()) // 因为还没聚合完，暂时不输出结果
    }

    override fun setSelfOutputNames() {
        setOutputNames(columns.map { it.toString() })
    }
}