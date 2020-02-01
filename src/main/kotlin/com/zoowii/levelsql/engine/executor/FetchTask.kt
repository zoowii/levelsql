package com.zoowii.levelsql.engine.executor

import com.zoowii.levelsql.engine.types.Chunk
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import java.util.concurrent.FutureTask

// 下级planner可以调用FetchTask的方法通知上级chunk提交/sourceEnd/error三种事件。上下级planner可能运行在不同线程中
class FetchTask {
    var future: CompletableFuture<FetchTask>? = null // 用来等待未来处理结果的future对象

    var chunk: Chunk? = null // 下级完成一次输出后把结果写入chunk
    var sourceEnd = false // 下级的输入是否正常结束
    var error: String? = null // 下级处理遇到错误时

    fun submitChunk(chunk: Chunk) {
        this.chunk = chunk
        future?.complete(this)
    }

    fun submitSourceEnd() {
        this.sourceEnd = true
        future?.complete(this)
    }

    fun submitError(err: String) {
        this.error = err
        future?.complete(this)
    }

    // 等待未来处理结果
    fun waitFuture(): Future<FetchTask> {
        future = CompletableFuture<FetchTask>()
        return future!!
    }

    fun isEnd(): Boolean {
        return chunk != null || sourceEnd || error != null
    }
}