package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import java.util.concurrent.Future


// 创建数据库的planner
class CreateDatabasePlanner(private val sess: DbSession, val dbName: String) : LogicalPlanner(sess) {
    private var executed = false

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if (executed) {
            fetchTask.submitSourceEnd()
            return
        }
        executed = true
        try {
            val db = sess.engine.createDatabase(dbName)
            db.saveMeta()
            sess.engine.saveMeta()
            fetchTask.submitChunk(Chunk.singleLongValue(1))
        } catch (e: Exception) {
            fetchTask.submitError(e.message!!)
        }
    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask, childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask, childrenFetchTasks: List<FetchTask>) {
        simplePassChildrenTasks(fetchTask, childrenFetchTasks)
    }

    override fun setSelfOutputNames() {
        setOutputNames(listOf("count"))
    }

}
