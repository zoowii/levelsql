package com.zoowii.levelsql.test

import com.zoowii.levelsql.engine.*
import com.zoowii.levelsql.engine.store.IStore
import com.zoowii.levelsql.engine.store.LocalFileStore
import com.zoowii.levelsql.engine.utils.logger
import org.junit.Assert
import org.junit.Test
import java.io.File

class TransactionTests {
    private val log = logger()

    class TestSession(val sessionName: String) {
        var store: IStore? = null
    }

    private fun cleanAndInitStore(sess: TestSession, clean: Boolean = true) {
        val localDbFile = File("./tx_tests_local/" + sess.sessionName)
        if (clean && localDbFile.exists()) {
            localDbFile.deleteRecursively()
        }
        sess.store = LocalFileStore.openStore(localDbFile)
    }

    private fun createTestDatabase(sess: TestSession, engine: LevelSqlEngine): Database {
        engine.createDatabase("test1").saveMeta()
        engine.createDatabase("test2").saveMeta()
        val db = engine.createDatabase("test")
        engine.saveMeta()
        val sess: DbSession? = null
        val employeeTableColumns = listOf(
                TableColumnDefinition("id", IntColumnType(), true),
                TableColumnDefinition("name", VarCharColumnType(50), true),
                TableColumnDefinition("age", IntColumnType(), true)
        )
        val table = db.createTable(sess, "employee", employeeTableColumns, "id")
        table.createIndex(sess, "employee_name_idx", listOf("name"), false)
        db.saveMeta()
        return db
    }

    @Test
    fun testCommit() {
        val testSession = TestSession("testCommit")
        cleanAndInitStore(testSession, true)
        val store = testSession.store ?: return
        val engine = LevelSqlEngine(store)

        val db = createTestDatabase(testSession, engine)

        val dbSession = engine.createSession()
        dbSession.beginTransaction()

        dbSession.useDb(db.dbName)
        val sql1 = "insert into employee (id, name, age) values (1, 'hello', 18)"
        engine.executeSQL(dbSession, sql1)
        val sql2 = "select * from employee"
        val selectResult = engine.executeSQL(dbSession, sql2)
        log.info("select rows after insert before commit tx {}", selectResult.chunk.rows)
        Assert.assertEquals(selectResult.chunk.rows.size, 1)

        dbSession.commitTransaction()

        engine.shutdown()
    }

    @Test
    fun testRollback() {
        val testSession = TestSession("testCommit")
        cleanAndInitStore(testSession, true)
        val store = testSession.store ?: return
        val engine = LevelSqlEngine(store)

        val db = createTestDatabase(testSession, engine)

        val dbSession = engine.createSession()
        dbSession.beginTransaction()

        dbSession.useDb(db.dbName)
        val sql1 = "insert into employee (id, name, age) values (1, 'hello', 18)"
        engine.executeSQL(dbSession, sql1)
        val sql2 = "select * from employee"
        val selectResult = engine.executeSQL(dbSession, sql2)
        log.info("select rows after insert before commit tx {}", selectResult.chunk.rows)
        Assert.assertEquals(selectResult.chunk.rows.size, 1)

        dbSession.rollbackTransaction()

        val dbSession2 = engine.createSession()
        dbSession2.useDb(db.dbName)
        val selectResultAfterRollback = engine.executeSQL(dbSession2, sql2)
        log.info("select rows after insert after rollback tx {}", selectResultAfterRollback.chunk.rows)
        Assert.assertEquals(selectResultAfterRollback.chunk.rows.size, 0)


        engine.shutdown()
    }
}