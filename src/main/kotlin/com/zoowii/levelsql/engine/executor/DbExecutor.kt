package com.zoowii.levelsql.engine.executor

// planner树的执行器和调度器，实际执行planner树
// 一次sql的执行，每个planner都可能被调用多次。上层planner给下层planner提交一个start请求，然后可能多次给下层planner提交一个输出的request(FetchTask)
// 下层planner在start后，如果收到输出的request，则继续执行逻辑并填充输出数据到输出的request(FetchTask)。直到没数据或者错误后提交done/error给FutureTask
class DbExecutor {
    // TODO
}