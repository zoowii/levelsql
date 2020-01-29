package com.zoowii.levelsql.sql.parser

import com.zoowii.levelsql.engine.exceptions.SqlParseException
import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.sql.ast.*
import com.zoowii.levelsql.sql.scanner.Rune
import com.zoowii.levelsql.sql.scanner.Scanner
import com.zoowii.levelsql.sql.scanner.Token
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkAlter
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkCreate
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkDatabase
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkDelete
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkDescribe
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkEOS
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkFrom
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkInsert
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkName
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkSelect
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkShow
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkTable
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkUpdate
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkWhere
import java.io.InputStream

class SqlParser(private val source: String, private val reader: InputStream) {
    private val log = logger()

    private val scanner = Scanner(source, reader)

    private val parsedStatements = mutableListOf<Statement>()
    private val currentStatement: Statement? = null

    fun parse() {
        scanner.next()
        statementList()
        scanner.check(tkEOS)
    }

    private fun next() {
        scanner.next()
    }

    private fun check(t: Rune) {
        scanner.check(t)
    }

    private fun checkNext(t: Rune) {
        check(t)
        next()
    }

    private fun checkNext(c: Char) {
        checkNext(c.toInt())
    }

    private fun checkName(): String {
        check(tkName)
        val s = currentToken().s
        next()
        return s
    }

    private fun testNext(t: Rune): Boolean {
        return scanner.testNext(t)
    }

    private fun testNext(c: Char): Boolean {
        return testNext(c.toInt())
    }

    private fun currentToken(): Token {
        return scanner.currentToken()
    }

    private fun statementList() {
        var count = 0
        val maxLoop = 10000
        while (!scanner.eos()) {
            count++
            statement()
            testNext(',')
        }
        if(!scanner.eos())
            throw SqlParseException("too many statements")
    }

    private fun addSqlStatement(stmt: Statement) {
        parsedStatements += stmt
    }

    private fun showStatement(line: Int) {
        next()
        val name = checkName()
        when {
            name == "databases" -> {
                log.debug("found show databases sql")
                addSqlStatement(ShowStatement(line, name))
            }
            name == "tables" -> {
                log.debug("found show tables sql")
                addSqlStatement(ShowStatement(line, name))
            }
            else -> {
                throw SqlParseException("invalid show statement")
            }
        }
    }

    private fun describeStatement(line: Int) {
        next()
        val tblName = checkTableName()
        addSqlStatement(DescribeTableStatement(line, tblName))
    }

    private fun createStatement(line: Int) {
        next()
        val typeToken = currentToken()
        next()
        when(typeToken.t) {
            tkDatabase -> {
                log.debug("create database sql")
                val dbName = checkName()
                addSqlStatement(CreateDatabaseStatement(line, dbName))
            }
            tkTable -> {
                log.debug("create table sql")
                val tblName = checkName()
                checkNext('(')
                val columns = mutableListOf<SqlColumnDef>()
                while(currentToken().t != ')'.toInt()) {
                    val colName = checkName()
                    val colType = checkName() // 暂时字段类型只接受单符号，比如 int, decimal, varchar, text等
                    columns += SqlColumnDef(colName, colType)
                    if(currentToken().t == ','.toInt()) {
                        next()
                    }
                }
                checkNext(')')
                addSqlStatement(CreateTableStatement(line, tblName, columns))
            }
            else -> {
                throw SqlParseException("invalid create sql type $typeToken")
            }
        }
    }

    // 条件表达式，比如用于where子句中
    private fun checkCondExpr(): CondExpr {
        // TODO: 用栈来构造表达式. 暂时只支持 <token> <op> <token> 格式
        when {
            currentToken().t == tkName -> {
                val left = currentToken()
                next()
                val op = currentToken()
                next()
                val right = currentToken()
                next()
                return BinOpExpr(op, TokenExpr(left), TokenExpr(right))
            }
            else -> {
                throw SqlParseException("not supported cond expr ${currentToken()}")
            }
        }
    }

    private fun checkWhereSubQuery(line: Int): WhereSubQuery {
        return WhereSubQuery(checkCondExpr())
    }

    private fun checkTableName(): String {
        return checkName()
    }

    private fun deleteStatement(line: Int) {
        next()
        checkNext(tkFrom)
        val tblName = checkTableName()
        val where: WhereSubQuery?
        if(testNext(tkWhere)) {
            where = checkWhereSubQuery(line)
        } else {
            where = null
        }
        log.debug("found delete sql")
        addSqlStatement(DeleteStatement(line, tblName, where))
    }

    private fun selectStatement(line: Int) {
        // TODO
    }

    private fun alterStatement(line: Int) {
        // TODO
    }

    private fun insertStatement(line: Int) {
        // TODO
    }

    private fun updateStatement(line: Int) {
        // TODO
    }

    private fun statement() {
        val line = scanner.lineNumber
        when (scanner.currentToken().t) {
            ';'.toInt() -> {
                scanner.next()
            }
            tkShow -> {
                showStatement(line)
            }
            tkCreate -> {
                createStatement(line)
            }
            tkInsert -> {
                insertStatement(line)
            }
            tkUpdate -> {
                updateStatement(line)
            }
            tkDelete -> {
                deleteStatement(line)
            }
            tkSelect -> {
                selectStatement(line)
            }
            tkAlter -> {
                alterStatement(line)
            }
            tkDescribe -> {
                describeStatement(line)
            }
            else -> {
                throw SqlParseException("not support sql syntax ${scanner.currentToken()}")
            }
        }
    }

    fun getStatements(): List<Statement> {
        return parsedStatements
    }
}