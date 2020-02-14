package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.sql.ast.Expr
import java.util.concurrent.Future


// 设置数据库参数的算子
class SetDbParamPlanner(private val sess: DbSession, val paramName: String, val expr: Expr) : LogicalPlanner(sess) {
    private var executed = false

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if(executed) {
            fetchTask.submitSourceEnd()
            return
        }
        // TODO: 设置数据库系统的参数. 目前忽略不计,简单输出 expr结果
        val chunk = Chunk()
        chunk.rows = mutableListOf(Row())
        val paramValue = expr.eval(chunk, listOf())[0]
        val row = Row()
        chunk.rows[0].data = listOf(paramValue)
        executed = true
        fetchTask.submitChunk(chunk)
    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask, childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask, childrenFetchTasks: List<FetchTask>) {
        simplePassChildrenTasks(fetchTask, childrenFetchTasks)
    }

    override fun setSelfOutputNames() {
        setOutputNames(listOf())
    }

}