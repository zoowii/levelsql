package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.planner.Planner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Row
import java.util.concurrent.Future


// 笛卡尔积的planner
class ProductPlanner(private val sess: DbSession) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "product${childrenToString()}"
    }

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {

    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {

    }

    private val childrenChunks = HashMap<Planner, Chunk>() // 各children的子任务的累计输出
    private val childrenChunksSourceEnded = HashMap<Planner, Boolean>() // 各children的子任务是否结束
    private var evaluated = false

    // 对chunks做笛卡尔积
    private fun productChunks(chunks: List<Chunk>): Chunk {
        if (chunks.isEmpty()) {
            return Chunk()
        }
        if (chunks.size == 1) {
            return chunks[0]
        }
        val result = Chunk()
        val firstChunk = chunks[0]
        val remainingProductedChunks = productChunks(chunks.subList(1, chunks.size))
        for (i in 0 until remainingProductedChunks.rows.size) {
            for (j in 0 until firstChunk.rows.size) {
                val row = Row()
                row.data = firstChunk.rows[j].data + remainingProductedChunks.rows[i].data
                result.rows.add(row)
            }
        }
        return result
    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask,
                                        childrenFetchTasks: List<FetchTask>) {
        if (fetchTask.isEnd()) {
            return
        }
        if (evaluated) {
            fetchTask.submitSourceEnd()
            return
        }
        // 要等待多个children输入都sourceEnd后，对结果做笛卡尔积返回
        for (i in 0 until children.size) {
            val child = children[i]
            val childTask = childrenFetchTasks[i]
            if (childTask.error != null) {
                fetchTask.submitError(childTask.error!!)
                return
            }
            if (childTask.sourceEnd) {
                childrenChunksSourceEnded[child] = true
                continue
            }
            if (!childrenChunks.containsKey(child)) {
                childrenChunks[child] = Chunk()
            }
            val childrenChunk = childrenChunks[child]!!
            childrenChunk.rows.addAll(childTask.chunk!!.rows)
        }
        if (childrenChunksSourceEnded.size < children.size) {
            // children还没输出完，等输出完后再做笛卡尔积
            fetchTask.submitChunk(Chunk())
            return
        }
        val orderedChildrenChunks = mutableListOf<Chunk>()
        for (child in children) {
            orderedChildrenChunks.add(if(childrenChunks.containsKey(child)) childrenChunks[child]!! else Chunk())
        }
        // 对orderedChildrenChunks做笛卡尔积
        val productChunk = productChunks(orderedChildrenChunks)
        fetchTask.submitChunk(productChunk)
        evaluated = true
    }

    override fun setSelfOutputNames() {
        val outputNames = mutableListOf<String>()
        for (child in children) {
            outputNames.addAll(child.getOutputNames())
        }
        setOutputNames(outputNames)
    }
}