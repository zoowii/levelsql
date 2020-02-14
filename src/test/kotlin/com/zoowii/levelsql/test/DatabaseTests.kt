package com.zoowii.levelsql.test

import com.zoowii.levelsql.engine.IntColumnType
import com.zoowii.levelsql.engine.TableColumnDefinition
import com.zoowii.levelsql.engine.VarCharColumnType
import com.zoowii.levelsql.engine.LevelSqlEngine
import com.zoowii.levelsql.engine.store.IStore
import com.zoowii.levelsql.engine.store.LocalFileStore
import org.junit.Before
import org.junit.Test
import java.io.File

class DatabaseTests {
    private var store: IStore? = null

    @Before fun beforeTests() {
        val localDbFile = File("./engine_tests_local")
        store = LocalFileStore.openStore(localDbFile)
    }

    @Test fun testSaveEngine() {
        val engine = LevelSqlEngine(store!!)
        engine.createDatabase("test1").saveMeta()
        engine.createDatabase("test2").saveMeta()
        val db = engine.createDatabase("test")
        engine.saveMeta()
        val employeeTableColumns = listOf(
                TableColumnDefinition("id", IntColumnType(), true),
                TableColumnDefinition("name", VarCharColumnType(50), true),
                TableColumnDefinition("age", IntColumnType(), true)
        )
        val table = db.createTable("employee", employeeTableColumns, "id")
        table.createIndex("employee_name_idx", listOf("name"), false)
        db.saveMeta()
        println("engine saved $engine")
        println("db saved $db")
        engine.shutdown()
    }

    @Test fun testLoadEngine() {
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        println("engine: $engine")
        val testDb = engine.openDatabase("test")
        println("test db: $testDb")
        engine.shutdown()
    }
}