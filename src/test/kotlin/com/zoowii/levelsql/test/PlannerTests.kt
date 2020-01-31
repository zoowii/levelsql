package com.zoowii.levelsql.test

import com.zoowii.levelsql.engine.LevelSqlEngine
import com.zoowii.levelsql.engine.store.IStore
import com.zoowii.levelsql.engine.store.LocalFileStore
import org.junit.Before
import org.junit.Test
import java.io.File

class PlannerTests {
    private var store: IStore? = null

    @Before
    fun beforeTests() {
        val localDbFile = File("./planner_tests_local")
        store = LocalFileStore.openStore(localDbFile)
    }

    @Test fun testSimpleSelectLogicalPlanner() {
        val engine = LevelSqlEngine(store!!)
        val session = engine.createSession()

        val sql1 = "select name, age, * from employee, person left join country on employee.country_id=country.id " +
                "where age >= 18 order by id desc group by age limit 10,20"
        engine.executeSQL(session, sql1)
    }
}