package com.zoowii.levelsql.test.csv

import com.zoowii.levelsql.engine.LevelSqlEngine
import com.zoowii.levelsql.engine.planner.source.csv.CsvDbSession
import com.zoowii.levelsql.engine.store.DummyStore
import org.junit.Assert
import org.junit.Test

class CsvPlannerTests {
    @Test
    fun testSelectPlanner() {
        val store = DummyStore()
        val engine = LevelSqlEngine(store)
        engine.loadMeta()
        val session = CsvDbSession("test", "employee",
        listOf("name","age","country"), "csv_test_files/employee.csv")
        engine.bindExternalSession(session)
        session.useDb("test")
        val sql1 = "select name, age, country from employee"
        engine.executeSQL(session, sql1)
        engine.shutdown()
    }
}