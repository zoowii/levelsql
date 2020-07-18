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

class DescribeTablePlanner(private val sess: IDbSession, val tblName: String) : LogicalPlanner(sess) {
    private var executed = false

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if(executed) {
            fetchTask.submitSourceEnd()
            return
        }
        sess as DbSession // TODO
        val db = sess.db
        if(db == null) {
            executed = true
            fetchTask.submitError("please select a database first")
            return
        }
        val table = db.findTable(tblName)
        if(table == null) {
            executed = true
            fetchTask.submitError("can't find table $tblName")
            return
        }
        val rows = mutableListOf<Row>()
        for(col in table.columns) {
            val row = Row()
            row.data = listOf(
                    Datum(DatumTypes.kindString, stringValue = col.name),
                    Datum(DatumTypes.kindString, stringValue = col.columnType.toString()))
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
        setOutputNames(listOf("name", "type"))
    }
}