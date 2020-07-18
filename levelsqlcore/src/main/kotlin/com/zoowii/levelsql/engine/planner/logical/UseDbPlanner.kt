package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import java.util.concurrent.Future

// use dbName语句的算子
class UseDbPlanner(private val sess: IDbSession, val dbName: String) : LogicalPlanner(sess) {
    private var executed = false

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if(executed) {
            fetchTask.submitSourceEnd()
            return
        }
        if(!sess.containsDb(dbName)) {
            fetchTask.submitError("can't find database $dbName")
            return
        }
        sess.useDb(dbName)
        executed = true
        val chunk = Chunk()
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