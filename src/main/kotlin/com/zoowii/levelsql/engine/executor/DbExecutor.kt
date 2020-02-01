package com.zoowii.levelsql.engine.executor

import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.planner.Planner
import com.zoowii.levelsql.engine.types.Chunk
import java.lang.Exception
import java.sql.SQLException
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

// planner树的执行器和调度器，实际执行planner树
// 一次sql的执行，每个planner都可能被调用多次。上层planner给下层planner提交一个start请求，然后可能多次给下层planner提交一个输出的request(FetchTask)
// 下层planner在start后，如果收到输出的request，则继续执行逻辑并填充输出数据到输出的request(FetchTask)。直到没数据或者错误后提交done/error给FutureTask
class DbExecutor {
    private val taskExecutor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors())

    fun executePlanner(planner: Planner): Chunk {
        when {
            LogicalPlanner::class.java.isAssignableFrom(planner.javaClass) -> {
                return executeLogicalPlanner(planner as LogicalPlanner)
            }
            else -> {
                throw SQLException("not supported planner type ${planner.javaClass} in db executor")
            }
        }
    }

    private fun startPlannerTree(planner: LogicalPlanner) {
        taskExecutor.submit(planner)
        for (child in planner.children) {
            startPlannerTree(child)
        }
    }

    private val executeEachPlannerTimeoutSeconds: Long = 10

    private fun executeLogicalPlanner(planner: LogicalPlanner): Chunk {
        val result = mutableListOf<Chunk>()
        startPlannerTree(planner)
        try {
            while (true) {
                val fetchFuture = planner.submitFetchTask()
                val fetchTask: FetchTask
                try {
                    fetchTask = fetchFuture.get(executeEachPlannerTimeoutSeconds, TimeUnit.SECONDS)
                } catch (e: Exception) {
                    throw e
                }
                if (!fetchFuture.isDone) {
                    break
                }
                if (fetchTask.sourceEnd) {
                    break
                }
                if (fetchTask.error != null) {
                    throw SQLException(fetchTask.error)
                }
                if (fetchTask.chunk == null) {
                    break
                }
                result.add(fetchTask.chunk!!)
            }
        } finally {
            planner.stop()
        }
        return Chunk.mergeChunks(result)
    }

    fun shutdown() {
        taskExecutor.shutdown()
    }
}