package com.zoowii.levelsql.engine.planner

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.types.Chunk
import java.lang.Exception
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

/**
 * 目前数据库计算引擎用的火山模型
 */

// 执行计划中的算子的接口
interface Planner : Runnable {
    // 本次执行的db session
    fun getSession(): DbSession

    // 输出各列的名称
    fun getOutputNames(): List<String>

    fun setOutputNames(names: List<String>)

    // 上级planner/executor提交FetchTask给这个planner，要求未来获取输出
    fun submitFetchTask(): Future<FetchTask>
}

abstract class LogicalPlanner(private val sess: DbSession) : Planner {
    var children = mutableListOf<LogicalPlanner>()
    fun setChild(index: Int, planner: LogicalPlanner) {
        children[index] = planner
    }

    fun addChild(planner: LogicalPlanner) {
        children.add(planner)
    }

    private var outputNames = listOf<String>()
    override fun getOutputNames(): List<String> {
        return outputNames
    }

    override fun setOutputNames(names: List<String>) {
        outputNames = names
    }

    override fun getSession(): DbSession {
        return sess
    }

    fun maxOneRow(): Boolean = false

    protected fun childrenToString(): String {
        if (children.isEmpty()) {
            return ""
        }
        return "\n${children.joinToString(",\t")}"
    }

    private val stopped = AtomicBoolean(false)

    override fun run() {
        while (!stopped.get()) {
            if(taskQueue.isEmpty()) {
                Thread.sleep(10)
                continue
            }
            val fetchTask = taskQueue.poll()
            beforeChildrenTasksSubmit(fetchTask)
            val childrenFetchFutures = submitFetchTaskToChildren()
            afterChildrenTasksSubmitted(fetchTask, childrenFetchFutures)
            val childrenFetchTasks = mutableListOf<FetchTask>()
            for (childFuture in childrenFetchFutures) {
                try {
                    childrenFetchTasks.add(childFuture.get(executeChildPlannerTimeoutSeconds, TimeUnit.SECONDS))
                } catch (e: Exception) {
                    fetchTask.submitError(e.message!!)
                    break
                }
            }
            afterChildrenTasksDone(fetchTask, childrenFetchTasks)
        }
    }

    fun stop() {
        stopped.set(true)
        for (child in children) {
            child.stop()
        }
    }

    // 在给children提交FetchTask之前的处理逻辑
    protected abstract fun beforeChildrenTasksSubmit(fetchTask: FetchTask)

    // 给各children提交各子任务后，本planner的处理逻辑
    protected abstract fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                                       childrenFetchFutures: List<Future<FetchTask>>)

    // children都处理完本次自己的工作后，本planner的处理逻辑
    protected abstract fun afterChildrenTasksDone(fetchTask: FetchTask,
                                                  childrenFetchTasks: List<FetchTask>)

    // @return Triple(merged-chunk, hasSourceEndChild, error)
    protected fun mergeChildrenChunks(childrenFetchTasks: List<FetchTask>): Triple<Chunk, Boolean, String?> {
        val chunks = mutableListOf<Chunk>()
        var hasSourceEnd = false
        for (childTask in childrenFetchTasks) {
            if (childTask.chunk != null) {
                chunks.add(childTask.chunk!!)
                continue
            }
            if (childTask.sourceEnd) {
                hasSourceEnd = true
                chunks.add(Chunk())
                continue
            }
            if (childTask.error != null) {
                return Triple(Chunk(), hasSourceEnd, childTask.error)
            }
        }
        val mergedChunks = Chunk.mergeChunks(chunks)
        return Triple(mergedChunks, hasSourceEnd, null)
    }

    // 简单转发children的结果，本身不做更多处理
    protected fun simplePassChildrenTasks(fetchTask: FetchTask,
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
        fetchTask.submitChunk(mergedChunk)
    }

    private val executeChildPlannerTimeoutSeconds: Long = 10

    private val taskQueue = ConcurrentLinkedQueue<FetchTask>()

    // 收到上级提交的FetchTask, 本身做处理，并且对children都产生新fetchTask提交给children
    override fun submitFetchTask(): Future<FetchTask> {
        // 改成新建fetchTask先加入本算子的信箱，等本算子自身线程依次处理.
        val fetchTask = FetchTask()
        taskQueue.add(fetchTask)
        val fetchFuture = fetchTask.waitFuture()
        return fetchFuture
    }

    protected fun submitFetchTaskToChildren(): List<Future<FetchTask>> {
        val tasks = mutableListOf<Future<FetchTask>>()
        for (child in children) {
            val fetchFuture = child.submitFetchTask()
            tasks.add(fetchFuture)
        }
        return tasks
    }

    // 根据自身属性和children的outputNames设置本planner自身的outputNames
    protected abstract fun setSelfOutputNames()

    // 自底向上设置整棵planner树的outputNames
    fun setTreeOutputNames() {
        for(child in children) {
            child.setTreeOutputNames()
        }
        setSelfOutputNames()
    }
}

abstract class PhysicalPlanner(private val sess: DbSession) : Planner {
    var children = mutableListOf<PhysicalPlanner>()
    fun setChild(index: Int, planner: PhysicalPlanner) {
        children[index] = planner
    }

    fun addChild(planner: PhysicalPlanner) {
        children.add(planner)
    }

    private var outputNames = listOf<String>()
    override fun getOutputNames(): List<String> {
        return outputNames
    }

    override fun getSession(): DbSession {
        return sess
    }

    override fun setOutputNames(names: List<String>) {
        outputNames = names
    }

    override fun run() {
        // TODO
    }

    override fun submitFetchTask(): Future<FetchTask> {
        val fetchTask = FetchTask()
        val fetchFuture = fetchTask.waitFuture()
        // TODO
        return fetchFuture
    }
}