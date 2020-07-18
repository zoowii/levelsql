package com.zoowii.levelsql.test.csv

import com.zoowii.levelsql.engine.IntColumnType
import com.zoowii.levelsql.engine.LevelSqlEngine
import com.zoowii.levelsql.engine.TextColumnType
import com.zoowii.levelsql.engine.VarCharColumnType
import com.zoowii.levelsql.engine.planner.source.csv.CsvColumnDefinition
import com.zoowii.levelsql.engine.planner.source.csv.CsvDatabaseDefinition
import com.zoowii.levelsql.engine.planner.source.csv.CsvDbSession
import com.zoowii.levelsql.engine.planner.source.csv.CsvTableDefinition
import com.zoowii.levelsql.engine.store.DummyStore
import com.zoowii.levelsql.engine.types.DatumType
import com.zoowii.levelsql.engine.types.DatumTypes
import org.junit.Assert
import org.junit.Test

class CsvPlannerTests {
    @Test
    fun testSelectPlanner() {
        val csvDb = CsvDatabaseDefinition("test",
                listOf(CsvTableDefinition(
                        "employee",
                        listOf(CsvColumnDefinition("name", VarCharColumnType(100)),
                                CsvColumnDefinition("age", IntColumnType()),
                                CsvColumnDefinition("country", TextColumnType())),
                        "csv_test_files/employee.csv")))
        val store = DummyStore()
        val engine = LevelSqlEngine(store)
        engine.loadMeta()
        val session = CsvDbSession(csvDb)
        engine.bindExternalSession(session)
        session.useDb("test")
        val sql1 = "select name, age, country, age*2 from employee"
        engine.executeSQL(session, sql1)
        engine.shutdown()
    }
}