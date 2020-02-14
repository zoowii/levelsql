package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.index.IndexNodeValue
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import com.zoowii.levelsql.engine.utils.KeyCondition
import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.sql.ast.BinOpExpr
import com.zoowii.levelsql.sql.ast.Expr
import com.zoowii.levelsql.sql.ast.TokenExpr
import com.zoowii.levelsql.sql.scanner.TokenTypes
import java.sql.SQLException
import java.util.concurrent.Future


// 从索引中检索数据的planner
class IndexSelectPlanner(private val sess: DbSession, val tblName: String, val indexName: String,
                         val asc: Boolean, val filterCondExpr: Expr) : LogicalPlanner(sess) {
    private val log = logger()

    override fun toString(): String {
        return "index select $tblName by index $indexName ${if (asc) "asc" else "desc"} $filterCondExpr ${childrenToString()}"
    }

    private var seekedPos: IndexNodeValue? = null // 上次已经检索到的记录的位置

    private var sourceEnd = false

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if (sess.db == null) {
            throw SQLException("database not opened. need use one-database")
        }
        if (sourceEnd) {
            fetchTask.submitSourceEnd()
            return
        }
        val table = sess.db!!.openTable(tblName)
        val index = table.openIndex(indexName) ?: throw SQLException("can't find index $indexName")
        // TODO: 从filterCondExpr中构造在index的tree种seek用的KeyCondition，然后在index中搜索
        var keyCondition: KeyCondition? = null
        // 为简化实现，目前只接受过滤条件是一项的 column op literalValue 的二元条件表达式 或者 a=xxx and b = xxx 这类 and 以及等于表达式且可以用二级索引的条件

        if (filterCondExpr.javaClass != BinOpExpr::class.java) {
            throw SQLException("为简化实现，目前只接受过滤条件是一项的 column op literalValue 的二元条件表达式")
        }
        filterCondExpr as BinOpExpr
        if (filterCondExpr.left.javaClass != TokenExpr::class.java) {
            throw SQLException("为简化实现，目前只接受过滤条件是一项的 column op literalValue 的二元条件表达式")
        }
        if (filterCondExpr.right.javaClass != TokenExpr::class.java) {
            throw SQLException("为简化实现，目前只接受过滤条件是一项的 column op literalValue 的二元条件表达式")
        }
        val filterCondLeftExpr = (filterCondExpr.left as TokenExpr).token
        if (filterCondLeftExpr.t != TokenTypes.tkName) {
            throw SQLException("为简化实现，目前只接受过滤条件是一项的 column op literalValue 的二元条件表达式")
        }
        val filterCondRightExpr = (filterCondExpr.right as TokenExpr).token
        if (!filterCondRightExpr.isLiteralValue()) {
            throw SQLException("为简化实现，目前只接受过滤条件是一项的 column op literalValue 的二元条件表达式")
        }
        val filterCondOp = filterCondExpr.op
        // 检查二元表达式左值是否是索引的第一列
        assert(index.columns.isNotEmpty())
        if (index.columns[0] != filterCondLeftExpr.s) {
            throw SQLException("column ${filterCondLeftExpr.s} not in index")
        }
        // TODO: filterRightValue要和index的key的结构一起得到真正的树中的key值。比如如果是联合索引，需要合并。如果是二级索引且key是字符串，需要裁减长度为固定长度
        val filterRightValue = filterCondRightExpr.getLiteralDatumValue()
        // TODO: 联合索引的时候，如果是 a = xxx and b = xxx的条件且符合索引，则构造联合IndexKey
        keyCondition = KeyCondition.createFromBinExpr(filterCondOp.opToken, filterRightValue)

        if(keyCondition==null) {
            throw SQLException("can't create KeyCondition from index condition")
        }

        if (seekedPos == null) {
            seekedPos = index.tree.seekByCondition(keyCondition)
        } else {
            if (asc) {
                seekedPos = index.tree.nextRecordPosition(seekedPos!!)
            } else {
                seekedPos = index.tree.prevRecordPosition(seekedPos!!)
            }
            if (seekedPos != null) {
                // 需要检查这条记录是否满足要求
                val record = seekedPos!!.leafRecord()
                if (!keyCondition.match(record.key)) {
                    seekedPos = null
                }
            }
        }
        if (seekedPos == null) {
            // seeked to end
            log.debug("table $tblName select end")
            sourceEnd = true
            fetchTask.submitSourceEnd()
            return
        }
        val record = seekedPos!!.leafRecord()
        val row = Row().fromBytes(ByteArrayStream(record.value))
        log.debug("index select planner fetched row: ${row}")
        fetchTask.submitChunk(Chunk().replaceRows(listOf(row)))
    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask,
                                        childrenFetchTasks: List<FetchTask>) {
        simplePassChildrenTasks(fetchTask, childrenFetchTasks)
    }

    override fun setSelfOutputNames() {
        if (sess.db == null) {
            return
        }
        val table = sess.db!!.openTable(tblName)
        val index = table.openIndex(indexName) ?: throw SQLException("can't find index $indexName")
        if(index.primary) {
            setOutputNames(table.columns.map { it.name })
        } else {
            setOutputNames(index.columns)
        }
    }
}
