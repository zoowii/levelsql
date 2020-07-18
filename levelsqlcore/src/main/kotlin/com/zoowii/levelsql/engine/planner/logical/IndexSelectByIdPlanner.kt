package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.Table
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.index.IndexNodeValue
import com.zoowii.levelsql.engine.index.datumsToIndexKey
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import com.zoowii.levelsql.engine.utils.EqualKeyCondition
import com.zoowii.levelsql.engine.utils.logger
import java.sql.SQLException
import java.util.concurrent.Future

// TODO: 支持多数据源
// 回表查询。从输入得到一个[primaryKey]的流，然后用主键索引查表并输出
class IndexSelectByIdPlanner(private val sess: DbSession, val tblName: String) : LogicalPlanner(sess) {
    private val log = logger()

    override fun toString(): String {
        return "index select $tblName primary index by children secondary index ${childrenToString()}"
    }

    private var seekedPos: IndexNodeValue? = null // 上次已经检索到的记录的位置

    private var sourceEnd = false

    private var table: Table? = null

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if (sess.db == null) {
            throw SQLException("database not opened. need use one-database")
        }
        if (sourceEnd) {
            fetchTask.submitSourceEnd()
            return
        }
        this.table = sess.db!!.openTable(tblName)
        val index = table!!.openPrimaryIndex()
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
        if(error!=null) {
            fetchTask.submitError(error)
            return
        }
        if(mergedChunk.rows.isEmpty() && hasSourceEnd) {
            fetchTask.submitSourceEnd()
            return
        }
        if(mergedChunk.rows.isEmpty()) {
            fetchTask.submitChunk(Chunk())
            return
        }
        val index = this.table!!.openPrimaryIndex()
        // 从输入中得到各 id，依次去索引检索
        val resultRows = mutableListOf<Row>()
        for(inputRow in mergedChunk.rows) {
            assert(inputRow.data.isNotEmpty())
            val idDatum = inputRow.data[0]
            val idIndexKey = datumsToIndexKey(idDatum)
            val keyCondition = EqualKeyCondition(idIndexKey)
            seekedPos = index.tree.seekByCondition(keyCondition)
            if (seekedPos == null) {
                // seeked to end
                log.debug("index-by-id from table $tblName select end")
                sourceEnd = true
                fetchTask.submitSourceEnd()
                return
            }
            val record = seekedPos!!.leafRecord()
            val row = Row().fromBytes(ByteArrayStream(record.value))
            log.debug("index select by id planner fetched row: ${row}")
            resultRows.add(row)
        }
        fetchTask.submitChunk(Chunk().replaceRows(resultRows))
    }

    override fun setSelfOutputNames() {
        if (sess.db == null) {
            return
        }
        val table = sess.db!!.openTable(tblName)
        setOutputNames(table.columns.map { it.name })
    }
}
