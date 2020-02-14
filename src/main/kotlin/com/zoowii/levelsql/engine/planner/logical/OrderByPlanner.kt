package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.logger
import java.util.concurrent.Future

// 排序的planner
class OrderByPlanner(private val sess: DbSession, val column: String, val asc: Boolean) : LogicalPlanner(sess) {
    private val log = logger()

    override fun toString(): String {
        return "order by $column ${if (asc) "asc" else "desc"}${childrenToString()}"
    }

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {

    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {

    }

    private val fetchedAllRowsChunk: Chunk = Chunk()

    private var fileSortFinished = false

    override fun afterChildrenTasksDone(fetchTask: FetchTask,
                                        childrenFetchTasks: List<FetchTask>) {
        // TODO: 如果下方来源是从索引来的数据（ordered chunk)则不再排序，否则应该持续取到所有数据后内存中排序
        if (fetchTask.isEnd()) {
            return
        }
        val isInputSortedAndSameSortWithSelf = false // 是否输入数据是排序好的并且排序方式和本sort planner一致。比如下方数据来自索引查询时
        if (isInputSortedAndSameSortWithSelf) {
            log.debug("sorted chunk no need to filesort")
            simplePassChildrenTasks(fetchTask, childrenFetchTasks)
            return
        }
        val (mergedChunk, hasSourceEnd, error) = mergeChildrenChunks(childrenFetchTasks)
        if (error != null) {
            fetchTask.submitError(error)
            return
        }
        if (mergedChunk.rows.isNotEmpty() || !hasSourceEnd) {
            // children还没输出完，需要累计起来
            fetchedAllRowsChunk.rows.addAll(mergedChunk.rows)
            fetchTask.submitChunk(Chunk()) // 暂时先输出一个空chunk
            return
        }
        if (fileSortFinished) {
            fetchTask.submitSourceEnd()
            return
        }
        assert(children.isNotEmpty())
        val childrenOutputNames = children[0].getOutputNames()
        // 对输入数据(来自children的输出)做排序

        val originRows = fetchedAllRowsChunk.rows
        val sortFun = { row: Row ->
            val orderColumnValue = row.getItem(childrenOutputNames, column)
            orderColumnValue
        }
        val sortedRows: List<Row>
        if (asc) {
            sortedRows = originRows.sortedBy(sortFun)
        } else {
            sortedRows = originRows.sortedByDescending(sortFun)
        }
        fetchTask.submitChunk(Chunk().replaceRows(sortedRows))
        fileSortFinished = true
    }

    override fun setSelfOutputNames() {
        assert(children.isNotEmpty())
        setOutputNames(children[0].getOutputNames())
    }
}