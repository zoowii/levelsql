package com.zoowii.levelsql.engine.planner

import com.zoowii.levelsql.TableColumnDefinition
import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.index.IndexNodeValue
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import com.zoowii.levelsql.engine.utils.KeyCondition
import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.sql.ast.BinOpExpr
import com.zoowii.levelsql.sql.ast.CondExpr
import com.zoowii.levelsql.sql.ast.JoinSubQuery
import com.zoowii.levelsql.sql.ast.TokenExpr
import com.zoowii.levelsql.sql.scanner.Token
import com.zoowii.levelsql.sql.scanner.TokenTypes
import java.sql.SQLException
import java.util.concurrent.Future

// 创建数据库的planner
class CreateDatabasePlanner(private val sess: DbSession, val dbName: String) : LogicalPlanner(sess) {
    private var executed = false

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if (executed) {
            fetchTask.submitSourceEnd()
            return
        }
        executed = true
        try {
            val db = sess.engine.createDatabase(dbName)
            db.saveMeta()
            sess.engine.saveMeta()
            fetchTask.submitChunk(Chunk.singleLongValue(1))
        } catch (e: Exception) {
            fetchTask.submitError(e.message!!)
        }
    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask, childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask, childrenFetchTasks: List<FetchTask>) {
        simplePassChildrenTasks(fetchTask, childrenFetchTasks)
    }

    override fun setSelfOutputNames() {
        setOutputNames(listOf("count"))
    }

}

// 创建table的planner
class CreateTablePlanner(private val sess: DbSession, val tblName: String, val columns: List<TableColumnDefinition>,
                         val primaryKey: String) : LogicalPlanner(sess) {
    private var executed = false

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if (executed) {
            fetchTask.submitSourceEnd()
            return
        }
        executed = true

        val db = sess.db ?: throw SQLException("database not opened. need use one-database")
        try {
            db.createTable(tblName, columns, primaryKey)
            db.saveMeta()
            fetchTask.submitChunk(Chunk.singleLongValue(1))
        } catch (e: Exception) {
            fetchTask.submitError(e.message!!)
        }
    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask, childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask, childrenFetchTasks: List<FetchTask>) {
        simplePassChildrenTasks(fetchTask, childrenFetchTasks)
    }

    override fun setSelfOutputNames() {
        setOutputNames(listOf("count"))
    }

}

// 创建索引的planner
class CreateIndexPlanner(private val sess: DbSession, val indexName: String, val tblName: String,
                         val columns: List<String>, val unique: Boolean) : LogicalPlanner(sess) {
    private var executed = false

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if (executed) {
            fetchTask.submitSourceEnd()
            return
        }
        executed = true

        val db = sess.db ?: throw SQLException("database not opened. need use one-database")
        try {
            val table = db.openTable(tblName)
            if (table.containsIndex(indexName)) {
                throw DbException("index name $indexName conflict")
            }
            table.createIndex(indexName, columns, unique)
            db.saveMeta()
            fetchTask.submitChunk(Chunk.singleLongValue(1))
        } catch (e: Exception) {
            fetchTask.submitError(e.message!!)
        }
    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask, childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask, childrenFetchTasks: List<FetchTask>) {
        simplePassChildrenTasks(fetchTask, childrenFetchTasks)
    }

    override fun setSelfOutputNames() {
        setOutputNames(listOf("count"))
    }

}

// insert记录的planner
class InsertPlanner(private val sess: DbSession, val tblName: String, val columns: List<String>,
                    val rows: List<List<Token>>) : LogicalPlanner(sess) {
    private var executed = false

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if (executed) {
            fetchTask.submitSourceEnd()
            return
        }
        executed = true
        if (sess.db == null) {
            throw SQLException("database not opened. need use one-database")
        }
        val table = sess.db!!.openTable(tblName)
        // 目前rows中的Token只接受基本类型字面量直接传值
        val datumRows = rows.map {
            val row = it
            row.map {
                when {
                    it.isLiteralValue() -> it.getLiteralDatumValue()
                    else -> throw SQLException("unknown datum from token type ${it.t}")
                }
            }
        }
        // TODO: 把rows根据columns顺序和table的结构重排序，填充入没提高的自动填充的值和默认值，构成List<Row>
        for (datumRow in datumRows) {
            // 对插入的各记录，找到主键的值
            val primaryKeyIndex = columns.indexOf(table.primaryKey)
            if (primaryKeyIndex < 0)
                throw SQLException("now must insert into table with primary key value")
            if (datumRow.size != columns.size) {
                throw SQLException("row value count not equal to columns count")
            }
            val primaryKeyValue = datumRow[primaryKeyIndex]
            val row = Row()
            row.data = datumRow
            table.rawInsert(primaryKeyValue.toBytes(), row.toBytes())
        }
        fetchTask.submitChunk(Chunk.singleLongValue(datumRows.size.toLong())) // 输出添加的行数
    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask, childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask, childrenFetchTasks: List<FetchTask>) {
        simplePassChildrenTasks(fetchTask, childrenFetchTasks)
    }

    override fun setSelfOutputNames() {
        setOutputNames(listOf("count"))
    }

}

// 从table中检索数据的planner
class SelectPlanner(private val sess: DbSession, val tblName: String) : LogicalPlanner(sess) {
    private val log = logger()

    override fun toString(): String {
        return "select $tblName${childrenToString()}"
    }

    private var seekedPos: IndexNodeValue? = null // 目前已经遍历到的记录的位置

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
        if (seekedPos == null) {
            seekedPos = table.rawSeekFirst()
        } else {
            seekedPos = table.rawNextRecord(seekedPos)
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
        log.debug("select planner fetched row: ${row}")
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
        setOutputNames(table.columns.map { it.name })
    }
}

// 从索引中检索数据的planner
class IndexSelectPlanner(private val sess: DbSession, val tblName: String, val indexName: String,
                         val asc: Boolean, val filterCondExpr: CondExpr) : LogicalPlanner(sess) {
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
        // 为简化实现，目前只接受过滤条件是一项的 column op literalValue 的二元条件表达式
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
        val keyCondition = KeyCondition.createFromBinExpr(filterCondOp, filterRightValue)
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
        // TODO
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
        setOutputNames(table.columns.map { it.name })
    }
}

// 聚合操作的planner
class AggregatePlanner(private val sess: DbSession, val funcName: String, val column: String) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "aggregate $funcName($column)${childrenToString()}"
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
        setOutputNames(listOf("$funcName($column)"))
    }
}

// 从输入中投影出部分列的planner
class ProjectionPlanner(private val sess: DbSession, val columns: List<String>) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "projection ${columns.joinToString(", ")}${childrenToString()}"
    }

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {

    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask,
                                        childrenFetchTasks: List<FetchTask>) {
        if (fetchTask.isEnd()) {
            return
        }
        val (mergedChunk, hasSourceEnd, error) = mergeChildrenChunks(childrenFetchTasks)
        if (error != null) {
            fetchTask.submitError(error)
            return
        }
        if (mergedChunk.rows.isEmpty() && hasSourceEnd) {
            fetchTask.submitSourceEnd()
            return
        }
        assert(children.isNotEmpty())
        val childrenOutputNames = children[0].getOutputNames()
        val outputNames = getOutputNames()
        // 对输入数据(来自children的输出)做projection
        val projectionRows = mergedChunk.rows.map {
            val row = it
            val mappedRow = Row()
            mappedRow.data = outputNames.map {
                val colName = it
                row.getItem(childrenOutputNames, colName)
            }
            mappedRow
        }
        fetchTask.submitChunk(Chunk().replaceRows(projectionRows))
    }

    override fun setSelfOutputNames() {
        // 如果不包含*，则output names是选择的列，如果包含*，则是选择的列 + children[0]（如果children非空）的各列
        if (!columns.contains("*")) {
            setOutputNames(columns)
            return
        }
        val outputNames = mutableListOf<String>()
        outputNames.addAll(columns.filter { it != "*" })
        if (children.isNotEmpty()) {
            outputNames.addAll(children[0].getOutputNames())
        }
        setOutputNames(outputNames)
    }
}

// join table操作的planner
class JoinPlanner(private val sess: DbSession, val joinConditions: List<JoinSubQuery>) : LogicalPlanner(sess) {
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

// 按条件过滤数据的planner
class FilterPlanner(private val sess: DbSession, val cond: CondExpr) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "filter by $cond${childrenToString()}"
    }

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {

    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {

    }

    override fun afterChildrenTasksDone(fetchTask: FetchTask,
                                        childrenFetchTasks: List<FetchTask>) {
        if (fetchTask.isEnd()) {
            return
        }
        val (mergedChunk, hasSourceEnd, error) = mergeChildrenChunks(childrenFetchTasks)
        if (error != null) {
            fetchTask.submitError(error)
            return
        }
        if (mergedChunk.rows.isEmpty() && hasSourceEnd) {
            fetchTask.submitSourceEnd()
            return
        }
        val outputNames = getOutputNames()
        // 对输入数据(来自children的输出)做过滤
        val filteredRows = mergedChunk.rows.filter {
            val row = it
            row.matchCondExpr(cond, outputNames)
        }
        fetchTask.submitChunk(Chunk().replaceRows(filteredRows))
    }

    override fun setSelfOutputNames() {
        assert(children.isNotEmpty())
        setOutputNames(children[0].getOutputNames())
    }
}

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

// 对输入数据进行分组的planner
class GroupByPlanner(private val sess: DbSession, val column: String) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "group by $column${childrenToString()}"
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

// 限制查询数据行数和偏移量的planner
class LimitPlanner(private val sess: DbSession, val offset: Long, val limit: Long) : LogicalPlanner(sess) {
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
            orderedChildrenChunks.add(childrenChunks[child]!!)
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