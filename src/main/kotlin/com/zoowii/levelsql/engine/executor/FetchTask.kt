package com.zoowii.levelsql.engine.executor

import com.zoowii.levelsql.engine.types.Chunk

class FetchTask {
    var chunk: Chunk? = null // 下级完成一次输出后把结果写入chunk
    var sourceEnd = false // 下级的输入是否正常结束
    var error: String? = null // 下级处理遇到错误时

    // TODO: 下级planner可以调用FetchTask的方法通知上级chunk提交/sourceEnd/error三种事件。上下级planner可能运行在不同线程中
    fun submitChunk(chunk: Chunk) {
        this.chunk = chunk
        // TODO: 通知上级planner
    }
    fun submitSourceEnd() {
        this.sourceEnd = true
        // TODO: 通知上级planner
    }
    fun submitError(err: String) {
        this.error = err
        // TODO: 通知上级planner
    }
}