package com.zoowii.levelsql.engine.planner

import com.zoowii.levelsql.TableColumnDefinition
import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.Index
import com.zoowii.levelsql.engine.Table
import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.index.IndexNodeValue
import com.zoowii.levelsql.engine.index.datumsToIndexKey
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import com.zoowii.levelsql.engine.utils.EqualKeyCondition
import com.zoowii.levelsql.engine.utils.KeyCondition
import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.sql.ast.*
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
            table.rawInsert(primaryKeyValue, row)
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

// 回表查询。从输入得到一个[primaryKey]的流，然后用主键索引查表并输出
class IndexSelectByIdPlanner(private val sess: DbSession, val tblName: String) : LogicalPlanner(sess) {
    private val log = logger()

    override fun toString(): String {
        return "index select $tblName primary index by children secondary index ${childrenToString()}"
    }

    private var seekedPos: IndexNodeValue? = null // 上次已经检索到的记录的位置

    private var sourceEnd = false

    private var table: Table? = null

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {
        if (sess.db == null) {
            throw SQLException("database not opened. need use one-database")
        }
        if (sourceEnd) {
            fetchTask.submitSourceEnd()
            return
        }
        this.table = sess.db!!.openTable(tblName)
        val index = table!!.openPrimaryIndex()
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
        if(error!=null) {
            fetchTask.submitError(error)
            return
        }
        if(mergedChunk.rows.isEmpty() && hasSourceEnd) {
            fetchTask.submitSourceEnd()
            return
        }
        if(mergedChunk.rows.isEmpty()) {
            fetchTask.submitChunk(Chunk())
            return
        }
        val index = this.table!!.openPrimaryIndex()
        // 从输入中得到各 id，依次去索引检索
        val resultRows = mutableListOf<Row>()
        for(inputRow in mergedChunk.rows) {
            assert(inputRow.data.isNotEmpty())
            val idDatum = inputRow.data[0]
            val idIndexKey = datumsToIndexKey(idDatum)
            val keyCondition = EqualKeyCondition(idIndexKey)
            seekedPos = index.tree.seekByCondition(keyCondition)
            if (seekedPos == null) {
                // seeked to end
                log.debug("index-by-id from table $tblName select end")
                sourceEnd = true
                fetchTask.submitSourceEnd()
                return
            }
            val record = seekedPos!!.leafRecord()
            val row = Row().fromBytes(ByteArrayStream(record.value))
            log.debug("index select by id planner fetched row: ${row}")
            resultRows.add(row)
        }
        fetchTask.submitChunk(Chunk().replaceRows(resultRows))
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

// 聚合操作的planner, @param columns 可能是group by中的列，也可能是聚合函数调用比如 count(1), sum(age)等
class AggregatePlanner(private val sess: DbSession, val columns: List<Expr>) : LogicalPlanner(sess) {
    override fun toString(): String {
        return "aggregate ${columns.joinToString(", ")}${childrenToString()}"
    }

    override fun beforeChildrenTasksSubmit(fetchTask: FetchTask) {

    }

    override fun afterChildrenTasksSubmitted(fetchTask: FetchTask,
                                             childrenFetchFutures: List<Future<FetchTask>>) {

    }

    private var aggregatedDone = false
    // 累计的分组聚合结果, columnIndex => 各分组暂时的聚合结果
    private var tmpAggregateColumnValues: MutableList<MutableList<Datum>> = mutableListOf()

    override fun afterChildrenTasksDone(fetchTask: FetchTask,
                                        childrenFetchTasks: List<FetchTask>) {
        if (fetchTask.isEnd()) {
            return
        }
        if (aggregatedDone) {
            fetchTask.submitSourceEnd()
            return
        }
        val groupIndex = 0
        // TODO: 实现分组后，聚合算子的输入要知道是哪个分组(方便并行执行不同分组的数据)
        // 目前因为还没有实现分组，所以多次输入都是同一个分组，要把结果记录下来等待累计值

        // 聚合函数要得到部分结果的时候可以先做部分运算，不是每次得到一个结果最终输出多个结果，也不是等到所有都输出完成再给结果. 聚合前需要先分组
        val (mergedChunk, hasSourceEnd, error) = mergeChildrenChunks(childrenFetchTasks)
        if (error != null) {
            fetchTask.submitError(error)
            return
        }
        if (mergedChunk.rows.isEmpty() && hasSourceEnd) {
            aggregatedDone = true
            // 把累计的分组聚合结果输出
            // 把列式输出转成行式输出
            val resultRows = mutableListOf<Row>()
            val rowsCount = tmpAggregateColumnValues.map { it.size }.min() ?: 0
            for (i in 0 until rowsCount) {
                val row = Row()
                row.data = tmpAggregateColumnValues.map { it[i] }
                resultRows.add(row)
            }
            fetchTask.submitChunk(Chunk().replaceRows(resultRows))
            return
        }
        assert(children.isNotEmpty())
        val childrenOutputNames = children[0].getOutputNames()

        // 要求输入是分组后的数据
        // 对columns各表达式用输入数据(来自children的输出)做eval
        // 向量化计算得到各列的输出
        try {

            // 先给tmpAggregateColumnValues 分配足够的空间
            while (tmpAggregateColumnValues.size < columns.size) {
                val tmpColumnValues = mutableListOf<Datum>()
                tmpAggregateColumnValues.add(tmpColumnValues)
            }
            for (tmpColumnValues in tmpAggregateColumnValues) {
                while (tmpColumnValues.size <= groupIndex) {
                    tmpColumnValues.add(Datum(DatumTypes.kindNull))
                }
            }
            columns.mapIndexed { index, column ->
                when {
                    FuncCallExpr::class.java.isAssignableFrom(column.javaClass)
                            && AggregateFunc::class.java.isAssignableFrom((column as FuncCallExpr).func!!.javaClass) -> {
                        // 是聚合函数调用
                        column as FuncCallExpr
                        column.aggregateEval(tmpAggregateColumnValues[index], groupIndex, mergedChunk, childrenOutputNames)
                    }
                    else -> {
                        val columnValues = column.eval(mergedChunk, childrenOutputNames)
                        tmpAggregateColumnValues[index][groupIndex] = if (columnValues.isEmpty())
                            Datum(DatumTypes.kindNull)
                        else
                            columnValues[0]
                    }
                }
            }
        } catch (e: Exception) {
            fetchTask.submitError(e.message!!)
            return
        }
        fetchTask.submitChunk(Chunk()) // 因为还没聚合完，暂时不输出结果
    }

    override fun setSelfOutputNames() {
        setOutputNames(columns.map { it.toString() })
    }
}

// 从输入中投影出部分列的planner
class ProjectionPlanner(private val sess: DbSession, val columns: List<Expr>) : LogicalPlanner(sess) {
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
        val outputExprs = getOutputExprs()


        val projectionRows = mutableListOf<Row>()

        // 对输入数据(来自children的输出)做projection
        // 向量化计算得到各列的输出
        try {
            val projectionColumns = outputExprs.map {
                it.eval(mergedChunk, childrenOutputNames)
            }
            // 把列式输出转成行式输出
            val rowsCount = projectionColumns.map { it.size }.min() ?: 0
            for (i in 0 until rowsCount) {
                val row = Row()
                row.data = projectionColumns.map { it[i] }
                projectionRows.add(row)
            }
        } catch (e: Exception) {
            fetchTask.submitError(e.message!!)
            return
        }
        fetchTask.submitChunk(Chunk().replaceRows(projectionRows))
    }

    fun hasAggregateFunc(): Boolean {
        return columns.any {
            it.javaClass == FuncCallExpr::class.java && (it as FuncCallExpr).func!!.isAggregateFunc()
        }
    }

    private fun getOutputExprs(): List<Expr> {
        // columns中某些项可能是复杂表达式，这种情况需要eval。也有的情况是聚合函数调用，需要对聚合函数中用到的各列eval
        val outputExprs = mutableListOf<Expr>()
        val inOutputColumns = mutableSetOf<String>()
        for (column in columns) {
            if (column.javaClass == FuncCallExpr::class.java) {
                column as FuncCallExpr
                // column中如果用到outputExprs中已经有的单符号列，则直接复用
                column.usingColumns().map {
                    // 先不管是哪个table
                    if (inOutputColumns.contains(it.column)) {
                        true
                    } else {
                        if (it.tblName != null) {
                            outputExprs.add(ColumnHintExpr(it.tblName, it.column))
                        } else {
                            outputExprs.add(ColumnHintExpr("", it.column))
                        }
                        inOutputColumns.add(it.column)
                    }
                }
                continue
            }
            // 如果不包含*，则output names是选择的列，如果包含*，则是选择的列 + children[0]（如果children非空）的各列
            if (column.javaClass == TokenExpr::class.java && (column as TokenExpr).token.t == '*'.toInt()) {
                assert(children.isNotEmpty())
                outputExprs.addAll(children[0].getOutputNames().map {
                    ColumnHintExpr("", it)
                })
                continue
            }
            outputExprs.add(column)
        }
        return outputExprs
    }

    override fun setSelfOutputNames() {
        val outputNames = getOutputExprs().map { it.toString() }
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
class FilterPlanner(private val sess: DbSession, val cond: Expr) : LogicalPlanner(sess) {
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