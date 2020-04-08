package com.zoowii.levelsql.engine.planner.logical

import com.zoowii.levelsql.engine.DbSession
import com.zoowii.levelsql.engine.executor.FetchTask
import com.zoowii.levelsql.engine.planner.LogicalPlanner
import com.zoowii.levelsql.engine.types.Chunk
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.sql.scanner.Token
import com.zoowii.levelsql.sql.ast.Expr
import java.sql.SQLException
import java.util.concurrent.Future

// insert记录的planner
class InsertPlanner(private val sess: DbSession, val tblName: String, val columns: List<String>,
                    val rows: List<List<Expr>>) : LogicalPlanner(sess) {
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
                // eval expr
                val chunk = Chunk.singleLongValue(0L)
                val exprValueChunk = it.eval(chunk, listOf<String>())
                exprValueChunk[0]
                // when {
                //     it.isLiteralValue() -> it.getLiteralDatumValue()
                //     else -> throw SQLException("unknown datum from token type ${it.t}")
                // }
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
        setOutputNames(listOf())
    }

}