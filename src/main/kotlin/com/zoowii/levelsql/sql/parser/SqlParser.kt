package com.zoowii.levelsql.sql.parser

import com.zoowii.levelsql.engine.exceptions.SqlParseException
import com.zoowii.levelsql.sql.scanner.Scanner
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkAlter
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkCreate
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkDelete
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkEOS
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkInsert
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkName
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkSelect
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkShow
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkUpdate
import java.io.InputStream

class SqlParser(private val source: String, private val reader: InputStream) {
    private val scanner = Scanner(source, reader)

    fun parse() {
        scanner.next()
        statementList()
        scanner.check(tkEOS)
    }

    private fun next() {
        scanner.next()
    }

    private fun statementList() {
        var count = 0
        val maxLoop = 10000
        while (!scanner.eos()) {
            count++
            statement()
            if (scanner.currentToken().t == ';'.toInt()) {
                next()
            }
        }
        if(!scanner.eos())
            throw SqlParseException("too many statements")
    }

    private fun showStatement(line: Int) {
        // TODO
        next()
        scanner.check(tkName)
        val token = scanner.currentToken()
        next()
        when {
            token.s == "databases" -> {
                println("found show databases sql")
            }
            token.s == "tables" -> {
                println("found show tables sql")
            }
            else -> {
                throw SqlParseException("invalid show statement")
            }
        }
    }

    private fun createStatement(line: Int) {
        // TODO
    }

    private fun insertStatement(line: Int) {
        // TODO
    }

    private fun updateStatement(line: Int) {
        // TODO
    }

    private fun deleteStatement(line: Int) {
        // TODO
    }

    private fun selectStatement(line: Int) {
        // TODO
    }

    private fun alterStatement(line: Int) {
        // TODO
    }

    private fun statement() {
        val line = scanner.lineNumber
        // TODO: 创建一个新SQL AST
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
            else -> {
                throw SqlParseException("not support sql syntax ${scanner.tokenToString(scanner.currentToken().t)}")
            }
        }
    }
}