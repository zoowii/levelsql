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
import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.sql.ast.CondExpr
import com.zoowii.levelsql.sql.ast.JoinSubQuery
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
                         val indexColumns: List<String>) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "index select $tblName by index $indexName (${indexColumns.joinToString(", ")})${childrenToString()}"
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

// 按条件过滤的planner
class OrderByPlanner(private val sess: DbSession, val column: String, val asc: Boolean) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "order by $column ${if (asc) "asc" else "desc"}${childrenToString()}"
    }

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        // TODO
    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {
        // TODO
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

// 笛卡尔积的planner
class ProductPlanner(private val sess: DbSession) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "product${childrenToString()}"
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
        val outputNames = mutableListOf<String>()
        for (child in children) {
            outputNames.addAll(child.getOutputNames())
        }
        setOutputNames(outputNames)
    }
}