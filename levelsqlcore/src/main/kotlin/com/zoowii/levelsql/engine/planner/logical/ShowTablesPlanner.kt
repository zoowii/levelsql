package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import java.util.concurrent.Future


// 查询数据库中table列表的算子
class ShowTablesPlanner(private val sess: IDbSession) : LogicalPlanner(sess) {
    private var executed = false
    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if(executed) {
            fetchTask.submitSourceEnd()
            return
        }
        sess as DbSession // TODO
        val db = sess.db
        if(db == null) {
            fetchTask.submitError("please select a db first")
            return
        }
        val tables = db.listTables()
        val rows = mutableListOf<Row>()
        for(tblName in tables) {
            val row = Row()
            row.data = listOf(Datum(DatumTypes.kindString, stringValue = tblName))
            rows.add(row)
        }
        val chunk = Chunk().replaceRows(rows)
        executed = true
        fetchTask.submitChunk(chunk)
    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask, childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask, childrenFetchTasks: List<FetchTask>) {
        simplePassChildrenTasks(fetchTask, childrenFetchTasks)
    }

    override fun setSelfOutputNames() {
        setOutputNames(listOf("name"))
    }

}