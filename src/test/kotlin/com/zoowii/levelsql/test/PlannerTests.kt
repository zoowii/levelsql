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

        val testDbName = "test"
        val db: Database
        if (engine.containsDatabase(testDbName)) {
            db = engine.openDatabase(testDbName)
        } else {
            db = engine.createDatabase(testDbName)
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

        db.saveMeta()

        run {
            val session = engine.createSession()
            session.useDb(testDbName)
            engine.executeSQL(session, "create table person (id int, person_name text)")
            engine.executeSQL(session, "create index person_name_idx on person (per_name)")
        }

        run {
            val session = engine.createSession()
            session.useDb(testDbName)
            engine.executeSQL(session, "create table country (id int, country_name text)")
            engine.executeSQL(session, "create index country_name_idx on country (country_name)")
        }

        run {
            val session = engine.createSession()
            session.useDb(testDbName)
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
        val sql1 = "select name, age, * from employee where id > 1 order by id desc limit 1,2"
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