package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.index.IndexNodeValue
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.planner.source.RowWithPosition
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import com.zoowii.levelsql.engine.utils.logger
import java.sql.SQLException
import java.util.concurrent.Future


// 从table中检索数据的planner
class SelectPlanner(private val sess: IDbSession, val tblName: String) : LogicalPlanner(sess) {
    private val log = logger()

    override fun toString(): String {
        return "select $tblName${childrenToString()}"
    }

//    private var seekedPos: IndexNodeValue? = null // 目前已经遍历到的记录的位置
    private var seekedPos: RowWithPosition? = null // 目前已经遍历到的记录的位置

    private var sourceEnd = false

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if (!sess.verifyDbOpened()) {
            throw SQLException("database not opened. need use one-database")
        }
        if (sourceEnd) {
            fetchTask.submitSourceEnd()
            return
        }
        val engineSource = sess.getSqlEngineSource() ?: throw SQLException("engineSource not set for sql engine")
        val tableSource = engineSource.openTable(sess, tblName) ?: throw SQLException("open table $tblName error")
        if(seekedPos==null) {
            seekedPos = tableSource.seekFirst(sess)
        } else {
            seekedPos = tableSource.seekNextRecord(sess, seekedPos!!)
        }
//        val table = sess.db!!.openTable(tblName)
//        if (seekedPos == null) {
//            seekedPos = table.rawSeekFirst(sess)
//        } else {
//            seekedPos = table.rawNextRecord(sess, seekedPos)
//        }
        if (seekedPos == null) {
            // seeked to end
            log.debug("table $tblName select end")
            sourceEnd = true
            fetchTask.submitSourceEnd()
            return
        }
//        val record = seekedPos!!.leafRecord()
//        val row = Row().fromBytes(ByteArrayStream(record.value))
        val row = seekedPos!!.getRow()
        log.debug("select planner fetched row: $row")
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
        val engineSource = sess.getSqlEngineSource() ?: return
        val tableSource = engineSource.openTable(sess, tblName) ?: return
        val columns = tableSource.getColumns()
        setOutputNames(columns.map { it.name })
//        if (sess.db == null) {
//            return
//        }
//        val table = sess.db!!.openTable(tblName)
//        setOutputNames(table.columns.map { it.name })
    }
}