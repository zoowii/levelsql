package com.zoowii.levelsql.test

import com.zoowii.levelsql.IntColumnType
import com.zoowii.levelsql.TableColumnDefinition
import com.zoowii.levelsql.VarCharColumnType
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
        db.createTable("employee", employeeTableColumns)
        db.saveMeta()
        println("engine saved $engine")
        println("db saved $db")
    }

    @Test fun testLoadEngine() {
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        println("engine: $engine")
        val testDb = engine.openDatabase("test")
        println("test db: $testDb")
    }
}