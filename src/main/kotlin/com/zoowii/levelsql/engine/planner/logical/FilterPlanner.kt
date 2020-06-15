package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.sql.ast.Expr
import java.util.concurrent.Future


// 按条件过滤数据的planner
class FilterPlanner(private val sess: IDbSession, val cond: Expr) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "filter by $cond${childrenToString()}"
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
        val outputNames = getOutputNames()
        // 对输入数据(来自children的输出)做过滤
        val filteredRows = mergedChunk.rows.filter {
            val row = it
            row.matchCondExpr(cond, outputNames)
        }
        fetchTask.submitChunk(Chunk().replaceRows(filteredRows))
    }

    override fun setSelfOutputNames() {
        assert(children.isNotEmpty())
        setOutputNames(children[0].getOutputNames())
    }
}