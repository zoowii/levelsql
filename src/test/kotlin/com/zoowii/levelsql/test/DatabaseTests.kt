package com.zoowii.levelsql.test

import com.zoowii.levelsql.engine.*
import com.zoowii.levelsql.engine.store.IStore
import com.zoowii.levelsql.engine.store.LocalFileStore
import org.junit.Before
import org.junit.Test
import java.io.File

class DatabaseTests {

    class TestSession(val sessionName: String) {
        var store: IStore? = null
    }

    private fun cleanAndInitStore(sess: TestSession) {
        val localDbFile = File("./engine_tests_local/" + sess.sessionName)
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

    @Test fun testSaveEngine() {
        val sess = TestSession("testSaveEngine")
        cleanAndInitStore(sess)
        val store = sess.store ?: return

        val engine = LevelSqlEngine(store)

        val db = createTestDatabase(sess, engine)

        println("engine saved $engine")
        println("db saved $db")
        engine.shutdown()
    }

    @Test fun testLoadEngine() {
        val sess = TestSession("testLoadEngine")
        cleanAndInitStore(sess)
        val store = sess.store

        val engine = LevelSqlEngine(store!!)

        createTestDatabase(sess, engine)

        engine.loadMeta()
        println("engine: $engine")
        val testDb = engine.openDatabase("test")
        println("test db: $testDb")
        engine.shutdown()
    }
}