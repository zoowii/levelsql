package com.zoowii.levelsql.test

import com.zoowii.levelsql.IntColumnType
import com.zoowii.levelsql.TableColumnDefinition
import com.zoowii.levelsql.VarCharColumnType
import com.zoowii.levelsql.engine.Database
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

        val engine = LevelSqlEngine(store!!)

        run {
            if (!engine.containsDatabase("test1")) {
                val session = engine.createSession()
                engine.executeSQL(session, "create database test1")
            }
        }

        val db: Database
        if (engine.containsDatabase("test")) {
            db = engine.openDatabase("test")
        } else {
            db = engine.createDatabase("test")
        }
        engine.saveMeta()

        val employeeTableColumns = listOf(
                TableColumnDefinition("id", IntColumnType(), false),
                TableColumnDefinition("name", VarCharColumnType(50), true),
                TableColumnDefinition("age", IntColumnType(), true),
                TableColumnDefinition("country_id", IntColumnType(), true)
        )
        val employeeTable = db.createTable("employee", employeeTableColumns, "id")
        employeeTable.createIndex("employee_name_idx", listOf("name"), false)

        val personTableColumns = listOf(
                TableColumnDefinition("id", IntColumnType(), false),
                TableColumnDefinition("person_name", VarCharColumnType(50), true)
        )
        val personTable = db.createTable("person", personTableColumns, "id")
        personTable.createIndex("person_name_idx", listOf("person_name"), false)

        val countryTableColumns = listOf(
                TableColumnDefinition("id", IntColumnType(), false),
                TableColumnDefinition("country_name", VarCharColumnType(50), true)
        )
        val countryTable = db.createTable("country", countryTableColumns, "id")
        countryTable.createIndex("country_name_idx", listOf("country_name"), false)

        db.saveMeta()

        run {
            val session = engine.createSession()
            session.useDb("test")
            engine.executeSQL(session, "create table user (id int, name text, gender text)")
        }

        println("engine saved $engine")
        println("db saved $db")
    }

    @Test
    fun testSimpleSelectWithJoinLogicalPlanner() {
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        val sql1 = "select name, age, * from employee, person left join country on employee.country_id=country.id " +
                "where age >= 18 order by id desc group by age limit 10,20"
        engine.executeSQL(session, sql1)
    }

    @Test
    fun testSimpleSelectLogicalPlanner() {
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        val sql1 = "select name, age, * from employee where id > 3 limit 1,2"
        engine.executeSQL(session, sql1)
    }

    @Test
    fun testInsertSqlLogicalPlanner() {
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        val sql1 = "insert into employee (id, name, age) values (1, 'zhang1', 21), (2, 'zhang2', 22), (3, 'zhang3', 23)"
        engine.executeSQL(session, sql1)
    }
}