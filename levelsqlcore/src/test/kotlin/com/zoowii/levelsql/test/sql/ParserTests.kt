package com.zoowii.levelsql.test.sql

import com.zoowii.levelsql.sql.parser.SqlParser
import org.junit.Test
import java.io.ByteArrayInputStream

class ParserTests {
    @Test fun testShowSql() {
        run {
            val sql1 = "show databases"
            val input1 = ByteArrayInputStream(sql1.toByteArray())
            val parser1 = SqlParser("test", input1)
            parser1.parse()
            val stmts1 = parser1.getStatements()
            println("stmts1:" + stmts1.joinToString("\n"))
        }

        run {
            val sql2 = "show tables"
            val input2 = ByteArrayInputStream(sql2.toByteArray())
            val parser2 = SqlParser("test", input2)
            parser2.parse()
            val stmts2 = parser2.getStatements()
            println("stmts2:" + stmts2.joinToString("\n"))
        }
    }

    @Test fun testDescribeSql() {
        val sql1 = "describe employee"
        val input1 = ByteArrayInputStream(sql1.toByteArray())
        val parser1 = SqlParser("test", input1)
        parser1.parse()
        val stmts1 = parser1.getStatements()
        println("stmts1:" + stmts1.joinToString("\n"))
    }

    @Test fun testCreateSql() {
        run {
            val sql1 = "create database test"
            val input1 = ByteArrayInputStream(sql1.toByteArray())
            val parser1 = SqlParser("test", input1)
            parser1.parse()
            val stmts1 = parser1.getStatements()
            println("stmts1:" + stmts1.joinToString("\n"))
        }

        run {
            val sql2 = "create table employee (id int, name text, age int)"
            val input2 = ByteArrayInputStream(sql2.toByteArray())
            val parser2 = SqlParser("test", input2)
            parser2.parse()
            val stmts2 = parser2.getStatements()
            println("stmts2:" + stmts2.joinToString("\n"))
        }

        run {
            val sql3 = "create index employee_name_idx on employee (name, age)"
            val input3 = ByteArrayInputStream(sql3.toByteArray())
            val parser3 = SqlParser("test", input3)
            parser3.parse()
            val stmts3 = parser3.getStatements()
            println("stmts3:" + stmts3.joinToString("\n"))
        }
    }

    @Test fun testDeleteSql() {
        val sql1 = "delete from employee where age >= 18"
        val input1 = ByteArrayInputStream(sql1.toByteArray())
        val parser1 = SqlParser("test", input1)
        parser1.parse()
        val stmts1 = parser1.getStatements()
        println("stmts1:" + stmts1.joinToString("\n"))
    }

    @Test fun testInsertSql() {
        val sql1 = "insert into employee (name, country, age) values ('wangwu', 'China', 18+1)," +
                "('wang6', 'America', 81)"
        val input1 = ByteArrayInputStream(sql1.toByteArray())
        val parser1 = SqlParser("test", input1)
        try{
            parser1.parse()
        } catch(e: Exception) {
            e.printStackTrace()
            throw e
        }
        val stmts1 = parser1.getStatements()
        println("stmts1:" + stmts1.joinToString("\n"))
    }

    @Test fun testUpdateSql() {
        val sql1 = "update employee set name = 'wang8', age = 30 where id=1"
        val input1 = ByteArrayInputStream(sql1.toByteArray())
        val parser1 = SqlParser("test", input1)
        parser1.parse()
        val stmts1 = parser1.getStatements()
        println("stmts1:" + stmts1.joinToString("\n"))
    }

    @Test fun testAlterSql() {
        run {
            val sql1 = "alter table employee add gender text, add age int"
            val input1 = ByteArrayInputStream(sql1.toByteArray())
            val parser1 = SqlParser("test", input1)
            parser1.parse()
            val stmts1 = parser1.getStatements()
            println("stmts1:" + stmts1.joinToString("\n"))
        }

        run {
            val sql2 = "alter table employee drop column gender"
            val input2 = ByteArrayInputStream(sql2.toByteArray())
            val parser2 = SqlParser("test", input2)
            parser2.parse()
            val stmts2 = parser2.getStatements()
            println("stmts2:" + stmts2.joinToString("\n"))
        }
    }

    @Test fun testSelectSql() {
        val sql1 = "select name, age, * from employee, person left join country on employee.country_id=country.id " +
                "where age >= 18 or (name != 'hello' and name != 'world') order by id desc group by age limit 10,20"
        val input1 = ByteArrayInputStream(sql1.toByteArray())
        val parser1 = SqlParser("test", input1)
        parser1.parse()
        val stmts1 = parser1.getStatements()
        println("stmts1:" + stmts1.joinToString("\n"))
    }

}