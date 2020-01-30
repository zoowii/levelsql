package com.zoowii.levelsql.test

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
        engine.createDatabase("test1")
        engine.createDatabase("test2")
        engine.createDatabase("test3")
        engine.saveMeta()
        println("engine saved $engine")
    }

    @Test fun testLoadEngine() {
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        println("engine: $engine")
    }
}