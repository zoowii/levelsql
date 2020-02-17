package com.zoowii.levelsql.test

import com.zoowii.levelsql.engine.IntColumnType
import com.zoowii.levelsql.engine.TableColumnDefinition
import com.zoowii.levelsql.engine.VarCharColumnType
import com.zoowii.levelsql.engine.Database
import com.zoowii.levelsql.engine.LevelSqlEngine
import com.zoowii.levelsql.engine.store.IStore
import com.zoowii.levelsql.engine.store.LocalFileStore
import org.junit.Test
import java.io.File

class PlannerTests {
    private var store: IStore? = null

    private fun createSampleDb() {
        val localDbFile = File("./planner_tests_local")
        if(localDbFile.exists()) {
            localDbFile.deleteRecursively()
        }
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
        employeeTable.createIndex("employee_name_age_idx", listOf("name", "age"), false)
        employeeTable.createIndex("employee_age_name_idx", listOf("age", "name"), false)

        db.saveMeta()

        run {
            val session = engine.createSession()
            session.useDb(testDbName)
            engine.executeSQL(session, "create table person (id int, person_name text)")
            engine.executeSQL(session, "create index person_name_idx on person (person_name)")
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
        engine.shutdown()
    }

    @Test fun testSimpleSelectWithJoinLogicalPlanner() {
        createSampleDb()
        insertSampleRecords()
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        val sql1 = "select name, age, * from employee, person left join country on employee.country_id=country.id " +
                "where age >= 18 order by id desc group by age limit 10,20"
        engine.executeSQL(session, sql1)
    }

    @Test fun testSimpleSelectLogicalPlanner() {
        createSampleDb()
        insertSampleRecords()
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        val sql1 = "select name, age, * from employee where id > 1 order by age desc limit 1,2"
        engine.executeSQL(session, sql1)
    }

    @Test fun testSimpleSelectByPrimaryIndexLogicalPlanner() {
        createSampleDb()
        insertSampleRecords()
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        val sql1 = "select name, age, * from employee where id > 1 order by id desc limit 1,2"
        engine.executeSQL(session, sql1)
    }

    @Test fun testSimpleSelectBySecondaryIndexLogicalPlanner() {
        createSampleDb()
        insertSampleRecords()
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        // 使用了二级索引和联合索引，会优化成使用二级索引并根据需要做回表查询
        val sql1 = "select name, age, * from employee where name = 'zhang3' and age = 23 order by id desc"
        engine.executeSQL(session, sql1)
    }

    @Test fun testSimpleSelectByMultiColumnsIndexButProvidePartialValuesLogicalPlanner() {
        createSampleDb()
        insertSampleRecords()
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        // 使用了二级索引和联合索引，会优化成使用二级索引并根据需要做回表查询
        // 因为用了联合索引，但是只提供了部分值(age)，还是会用对应的联合索引(age, name)，只要满足最左匹配原则
        val sql1 = "select name, age, * from employee where age = 23 order by id desc"
        engine.executeSQL(session, sql1)
    }

    private fun insertSampleRecords() {
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        run {
            val sql = "insert into employee (id, name, age, country_id) values (1, 'zhang1', 21, 1), (2, 'zhang2', 22, 2), (3, 'zhang3', 23, 1)"
            engine.executeSQL(session, sql)
        }
        run {
            val sql = "insert into person (id, name) values (1, 'person-1'), (2, 'person-2'), (3, 'person-3')"
            engine.executeSQL(session, sql)
        }
        engine.shutdown()
    }

    @Test fun testInsertSqlLogicalPlanner() {
        createSampleDb()
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        run {
            val sql = "insert into employee (id, name, age, country_id) values (1, 'zhang1', 21, 1), (2, 'zhang2', 22, 2), (3, 'zhang3', 23, 1)"
            engine.executeSQL(session, sql)
        }
        run {
            val sql = "insert into person (id, name) values (1, 'person-1'), (2, 'person-2'), (3, 'person-3')"
            engine.executeSQL(session, sql)
        }
        engine.shutdown()
    }

    @Test fun testProductSqlLogicalPlanner() {
        createSampleDb()
        insertSampleRecords()
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        val sql1 = "select * from employee, person"
        engine.executeSQL(session, sql1)
        engine.shutdown()
    }

    @Test fun testLimitSqlLogicalPlanner() {
        createSampleDb()
        insertSampleRecords()
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        val sql1 = "select * from employee, person limit 2,2"
        engine.executeSQL(session, sql1)
        engine.shutdown()
    }

    @Test fun testAggregateSqlLogicalPlanner() {
        createSampleDb()
        insertSampleRecords()
        val engine = LevelSqlEngine(store!!)
        engine.loadMeta()
        val session = engine.createSession()
        session.useDb("test")
        val sql1 = "select sum(age), count(age), max(age), min(age) from employee, person where id > 0 limit 2,2"
        engine.executeSQL(session, sql1)
        engine.shutdown()
    }
}