package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.IDbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.sql.ast.JoinSubQuery
import java.util.concurrent.Future


// join table操作的planner
class JoinPlanner(private val sess: IDbSession, val joinConditions: List<JoinSubQuery>) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "${joinConditions.joinToString(", ")}${childrenToString()}"
    }

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        // TODO
    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {
        // TODO
    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask,
                                        childrenFetchTasks: List<FetchTask>) {
        // TODO
        simplePassChildrenTasks(fetchTask, childrenFetchTasks)
    }

    override fun setSelfOutputNames() {
        assert(children.isNotEmpty())
        setOutputNames(children[0].getOutputNames())
    }
}
