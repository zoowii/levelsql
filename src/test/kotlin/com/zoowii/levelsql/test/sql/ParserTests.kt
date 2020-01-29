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
        parser2.parse()
    }

}