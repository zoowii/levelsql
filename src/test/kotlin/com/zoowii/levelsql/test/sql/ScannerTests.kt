package com.zoowii.levelsql.test.sql

import com.zoowii.levelsql.sql.scanner.Scanner
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkFrom
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkInsert
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkInto
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkName
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkSelect
import org.junit.Test
import java.io.ByteArrayInputStream

class ScannerTests {
    @Test fun testScanInsertSql() {
        val sql = "insert into employee (name, country, age) values ('wangwu', 'China', 18+1)," +
                "('wang6', 'America', 81)"
        val input = ByteArrayInputStream(sql.toByteArray())
        try {
            val scanner = Scanner("test", input)
            scanner.next()
            scanner.check(tkInsert)
            scanner.next()
            scanner.check(tkInto)
            scanner.next()
            scanner.check(tkName)
            scanner.currentToken().s == "employee"
            scanner.next()
            scanner.check('('.toInt())
        } finally {
            input.close()
        }
    }

    @Test fun testSelectSql() {
        val sql = "select name, age, count(*) from employee where age >= 18 and country == 'China'"
        val input = ByteArrayInputStream(sql.toByteArray())
        try {
            val scanner = Scanner("test", input)
            scanner.next()
            scanner.check(tkSelect)
            scanner.next()
            scanner.check(tkName)
            scanner.next()
            scanner.check(','.toInt())
            scanner.next() // age
            scanner.next() // ,
            scanner.next() // count
            scanner.next()
            scanner.check('('.toInt())
            scanner.next()
            scanner.check('*'.toInt())
            scanner.next() // )
            scanner.next() // from
            scanner.check(tkFrom)
        } finally {
            input.close()
        }
    }
}