package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.TableColumnDefinition
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import java.sql.SQLException
import java.util.concurrent.Future


// 创建table的planner
class CreateTablePlanner(private val sess: DbSession, val tblName: String, val columns: List<TableColumnDefinition>,
                         val primaryKey: String) : LogicalPlanner(sess) {
    private var executed = false

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if (executed) {
            fetchTask.submitSourceEnd()
            return
        }
        executed = true

        val db = sess.db ?: throw SQLException("database not opened. need use one-database")
        try {
            db.createTable(sess, tblName, columns, primaryKey)
            db.saveMeta()
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