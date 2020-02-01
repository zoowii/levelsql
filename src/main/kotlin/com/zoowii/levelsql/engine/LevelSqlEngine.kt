package com.zoowii.levelsql.engine

import com.zoowii.levelsql.engine.exceptions.DbException
import com.zoowii.levelsql.engine.executor.DbExecutor
import com.zoowii.levelsql.engine.store.*
import com.zoowii.levelsql.engine.utils.ByteArrayStream
import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.sql.PlannerBuilder
import com.zoowii.levelsql.sql.parser.SqlParser
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.sql.SQLException
import java.util.concurrent.ConcurrentHashMap

class LevelSqlEngine(val store: IStore) {
    private val log = logger()

    private var databases = listOf<Database>()
    private var sessions = ConcurrentHashMap<Long, DbSession>()

    // 从store中加载元信息
    fun loadMeta() {
        val metaBytes = store.get(metaStoreKey())
                ?: throw SQLException("load engine error")
        metaFromBytes(this, metaBytes)
        for (db in databases) {
            db.loadMeta()
        }
    }

    fun saveMeta() {
        val metaBytes = metaToBytes()
        store.put(metaStoreKey(), metaBytes)
    }

    private fun metaStoreKey(): StoreKey {
        return StoreKey("engine_meta", -1)
    }

    fun metaToBytes(): ByteArray {
        val out = ByteArrayOutputStream()
        out.write(databases.size.toBytes())
        for (db in databases) {
            out.write(db.dbName.toBytes())
        }
        return out.toByteArray()
    }

    companion object {
        fun metaFromBytes(engine: LevelSqlEngine, data: ByteArray): Pair<LevelSqlEngine, ByteArray> {
            val stream = ByteArrayStream(data)
            val dbsCount = stream.unpackInt32()
            val dbs = mutableListOf<Database>()
            for (i in 0 until dbsCount) {
                val dbName = stream.unpackString()
                dbs += Database(dbName, engine.store)
            }
            engine.databases = dbs
            return Pair(engine, stream.remaining)
        }
    }

    fun createDatabase(dbName: String): Database {
        if (databases.any { it.dbName == dbName }) {
            throw DbException("database ${dbName} existed before")
        }
        val db = Database(dbName, store)
        databases += db
        return db
    }

    fun openDatabase(dbName: String): Database {
        return databases.firstOrNull { it.dbName == dbName } ?: throw DbException("database ${dbName} not found")
    }

    override fun toString(): String {
        return "engine: \n${databases.map { "\t${it.dbName}" }.joinToString("\n")}"
    }

    fun createSession(): DbSession {
        val sess = DbSession(this)
        sessions[sess.id] = sess
        return sess
    }

    // 解析执行SQL语句的API
    fun executeSQL(session: DbSession, sqls: String) {
        if (session != sessions.getOrDefault(session.id, null)) {
            throw SQLException("not active db session")
        }
        val input = ByteArrayInputStream(sqls.toByteArray())
        val parser = SqlParser("session-${session.id}", input)
        parser.parse()
        val dbExecutor = DbExecutor()
        val stmts = parser.getStatements()
        // TODO: 把stmts各SQL语句依次转成planner交给executor处理
        for (stmt in stmts) {
            val logicalPlanner = PlannerBuilder.sqlNodeToPlanner(session, stmt)
            log.debug("logical planner:\n$logicalPlanner")
            // TODO: optimize, to physical planner, physical planner optimize
            // TODO: use executor to execute planner
            val chunk = dbExecutor.executePlanner(logicalPlanner)
            log.debug("result:\n${chunk.rows.joinToString("\n")}")
        }
        dbExecutor.shutdown()
    }

}