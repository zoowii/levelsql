package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Row
import java.util.concurrent.Future


// 限制查询数据行数和偏移量的planner
class LimitPlanner(private val sess: IDbSession, val offset: Long, val limit: Long) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "limit $offset, $limit${childrenToString()}"
    }

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {

    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {

    }

    private var skippedCount: Long = 0 // 把输入跳过的记录行数
    private var outputRowsCount: Long = 0 // 对上层输出的记录行数

    override fun afterChildrenTasksDone(fetchTask: FetchTask,
                                        childrenFetchTasks: List<FetchTask>) {
        assert(children.size == 1)
        if (fetchTask.isEnd()) {
            return
        }
        if (outputRowsCount >= limit) {
            fetchTask.submitSourceEnd()
            return
        }
        val childTask = childrenFetchTasks[0]
        if (childTask.error != null) {
            fetchTask.submitError(childTask.error!!)
            return
        }
        if (childTask.sourceEnd) {
            fetchTask.submitSourceEnd()
            return
        }
        val childChunk = childTask.chunk!!

        if (childChunk.rows.size + skippedCount <= offset) {
            skippedCount += childChunk.rows.size
            fetchTask.submitChunk(Chunk())
            return
        }
        var remainingRows: List<Row>
        if (skippedCount < offset) {
            // 需要在childChunk中跳过部分行，剩下的行需要输出
            remainingRows = childChunk.rows.subList((offset - skippedCount).toInt(), childChunk.rows.size)
            skippedCount = offset
        } else {
            remainingRows = childChunk.rows
        }
        if (remainingRows.size >= (limit - outputRowsCount)) {
            // 来做child的输入超过剩余需要输出的行数，需要裁减掉超过的部分
            remainingRows = remainingRows.subList(0, (limit - outputRowsCount).toInt())
        }

        fetchTask.submitChunk(Chunk().replaceRows(remainingRows))
        outputRowsCount += remainingRows.size
    }

    override fun setSelfOutputNames() {
        assert(children.isNotEmpty())
        setOutputNames(children[0].getOutputNames())
    }
}