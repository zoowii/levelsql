package com.zoowii.levelsql.test.sql

import com.zoowii.levelsql.sql.parser.SqlParser
import org.junit.Test
import java.io.ByteArrayInputStream

class ParserTests {
    @Test fun testShowSql() {
        val sql1 = "show databases"
        val sql2 = "show tables"
        val input1 = ByteArrayInputStream(sql1.toByteArray())
        val input2 = ByteArrayInputStream(sql2.toByteArray())
        val parser1 = SqlParser("test", input1)
        val parser2 = SqlParser("test", input2)
        parser1.parse()
        val stmts1 = parser1.getStatements()
        println("stmts1:" + stmts1.joinToString("\n"))
        parser2.parse()
        val stmts2 = parser1.getStatements()
        println("stmts2:" + stmts2.joinToString("\n"))
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
        val sql1 = "create database test"
        val sql2 = "create table employee (id int, name text, age int)"
        val input1 = ByteArrayInputStream(sql1.toByteArray())
        val input2 = ByteArrayInputStream(sql2.toByteArray())
        val parser1 = SqlParser("test", input1)
        val parser2 = SqlParser("test", input2)
        parser1.parse()
        val stmts1 = parser1.getStatements()
        println("stmts1:" + stmts1.joinToString("\n"))
        parser2.parse()
        val stmts2 = parser1.getStatements()
        println("stmts2:" + stmts2.joinToString("\n"))
    }

    @Test fun testDeleteSql() {
        val sql1 = "delete from employee where age >= 18"
        val input1 = ByteArrayInputStream(sql1.toByteArray())
        val parser1 = SqlParser("test", input1)
        parser1.parse()
        val stmts1 = parser1.getStatements()
        println("stmts1:" + stmts1.joinToString("\n"))
    }

}