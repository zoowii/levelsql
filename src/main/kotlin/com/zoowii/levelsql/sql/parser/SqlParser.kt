package com.zoowii.levelsql.sql.parser

import com.zoowii.levelsql.engine.exceptions.SqlParseException
import com.zoowii.levelsql.engine.utils.logger
import com.zoowii.levelsql.sql.ast.*
import com.zoowii.levelsql.sql.scanner.*
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkAdd
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkAlter
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkBy
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkColumn
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkCreate
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkDatabase
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkDelete
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkDesc
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkDescribe
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkDrop
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkEOS
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkFrom
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkFull
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkGroup
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkIndex
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkInner
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkInsert
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkInt
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkInto
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkJoin
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkLeft
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkLimit
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkName
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkOn
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkOrder
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkRight
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkSelect
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkSet
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkShow
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkTable
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkUpdate
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkValues
import com.zoowii.levelsql.sql.scanner.TokenTypes.tkWhere
import java.io.InputStream

class SqlParser(private val source: String, private val reader: InputStream) {
    private val log = logger()

    private val scanner = Scanner(source, reader)

    private val parsedStatements = mutableListOf<Node>()
    private val currentStatement: Node? = null

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
        val s = checkToken()
        return s.s
    }

    private fun checkToken(): Token {
        val token = currentToken()
        next()
        return token
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
            if (!testNext(';')) {
                break
            }
        }
        if (!scanner.eos())
            throw SqlParseException("too many statements ${currentToken()}")
    }

    private fun addSqlStatement(stmt: Node) {
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
        val typeToken = checkToken()
        when (typeToken.t) {
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
                while (currentToken().t != ')'.toInt()) {
                    val colName = checkColumnName()
                    val colType = checkName() // 暂时字段类型只接受单符号，比如 int, decimal, varchar, text等
                    columns += SqlColumnDef(colName, colType)
                    if (!testNext(',')) {
                        break
                    }
                }
                checkNext(')')
                addSqlStatement(CreateTableStatement(line, tblName, columns))
            }
            tkIndex -> {
                val indexName = checkName()
                checkNext(tkOn)
                val tblName = checkTableName()
                checkNext('(')
                val columns = mutableListOf<String>()
                while (currentToken().t != ')'.toInt()) {
                    val colName = checkColumnName()
                    columns += colName
                    if (!testNext(',')) {
                        break
                    }
                }
                checkNext(')')
                addSqlStatement(CreateIndexStatement(line, indexName, tblName, columns))
            }
            else -> {
                throw SqlParseException("invalid create sql type $typeToken")
            }
        }
    }

    // 表达式，比如用于where子句, select子句等中
    private fun checkExpr(): Expr {
        // 各操作符的优先级
        val opPriorities = hashMapOf(
                Pair(TokenTypes.tkAnd, 11),
                Pair(TokenTypes.tkOr, 11),
                Pair('*'.toInt(), 10),
                Pair('/'.toInt(), 10),
                Pair('%'.toInt(), 10),
                Pair('+'.toInt(), 5),
                Pair('-'.toInt(), 5),
                Pair('>'.toInt(), 7),
                Pair('<'.toInt(), 7),
                Pair('='.toInt(), 7),
                Pair(TokenTypes.tkNe, 7),
                Pair(TokenTypes.tkGL, 7),
                Pair(TokenTypes.tkGe, 7),
                Pair(TokenTypes.tkLe, 7)
        )
        val opsStack = mutableListOf<Expr?>() // 保存未完成的表达式的操作符的栈, 其中某项为null表示这项是 '('
        val suffixStack = mutableListOf<Expr>() // 中缀表达式对应的后缀表达式
        loop@ while(!scanner.eos()) {
            val token = currentToken()
            when {
                token.t == tkName -> {
                    // TODO: 对于 a.b这类columnHint 以及 func(arg)这类函数调用信息，也要处理
                    suffixStack.add(TokenExpr(checkToken()))
                }
                // TODO: 当是select后的表达式的时候，表达式中可以是*号，但是只能少数几种简单形式，比如 *, a.*, count(*) 等
                token.isLiteralValue() -> {
                    suffixStack.add(TokenExpr(checkToken()))
                }
                token.isBinExprOperatorToken() -> {
                    val opToken = checkToken()
                    var added = false
                    while(opsStack.isNotEmpty()) {
                        // 栈顶一定是操作符
                        val lastOp = opsStack.last()
                        val opPriority = opPriorities[opToken.t]!!
                        val lastOpPriority: Int
                        if(lastOp == null) {
                            // 遇到一个左括号。左括号有最高优先级
                            lastOpPriority = Int.MAX_VALUE
                        } else {
                            if (lastOp.javaClass != TokenExpr::class.java)
                                throw SqlParseException("invalid expr $token")
                            lastOp as TokenExpr
                            lastOpPriority = opPriorities[lastOp.token.t]!!
                        }
                        if (opPriority > lastOpPriority) {
                            // 新操作符优先级比之前的优先级高，压入操作符栈优先处理
                            opsStack.add(TokenExpr(opToken))
                            added = true
                            break
                        } else {
                            popFromList(opsStack)
                            if(lastOp != null) {
                                suffixStack.add(lastOp)
                            }
                        }
                    }
                    // 没有操作符了，相当于新操作符大于上一个操作符的优先级
                    if(!added) {
                        opsStack.add(TokenExpr(opToken))
                    }
                }
                token.t == '('.toInt() -> {
                    next()
                    opsStack.add(null)
                }
                token.t == ')'.toInt() -> {
                    next()
                    // 要求栈中上一个是一个值(非null)并且上上一个值是'(' (用null表示)
                    if(opsStack.size<1) {
                        throw SqlParseException("invalid expr $token")
                    }
                    while(opsStack.isNotEmpty()) {
                        val item = popFromList(opsStack)
                        if(item==null) { // 遇到左括号了
                            break
                        }
                        suffixStack.add(item)
                    }
                }
                else -> {
                    break@loop
                }
            }
        }
        // 把剩下操作符依次弹出
        while(opsStack.isNotEmpty()) {
            val item = popFromList(opsStack)
            if(item==null) {
                throw SqlParseException("invalid ( in sql")
            }
            suffixStack.add(item)
        }
        // 把suffixStack转成表达式树
        val exprsStack = mutableListOf<Expr>() // 构造表达式树过程中的操作树
        while(suffixStack.isNotEmpty()) {
            val item = unshiftFromList(suffixStack) // 当成前缀表达式来处理
            // 目前 item都是TokenExpr，因为还没有符合项比如 a.b, func(a)
            item as TokenExpr
            when {
                item.token.isBinExprOperatorToken() -> {
                    if(exprsStack.size<2) {
                        throw SqlParseException("invalid expr ${item.token}")
                    }
                    val right = popFromList(exprsStack)
                    val left = popFromList(exprsStack)
                    exprsStack.add(BinOpExpr(item.token, left, right))
                }
                else -> {
                    // 操作数
                    exprsStack.add(item)
                }
            }
        }

        if(exprsStack.size!=1 || (exprsStack.isNotEmpty() && exprsStack[0]==null))
            throw SqlParseException("invalid expr ${currentToken()}")
        return exprsStack[0]
    }

    private fun checkWhereSubQuery(line: Int): WhereSubQuery {
        return WhereSubQuery(checkExpr())
    }

    private fun checkTableName(): String {
        return checkName()
    }

    private fun deleteStatement(line: Int) {
        next()
        checkNext(tkFrom)
        val tblName = checkTableName()
        val where: WhereSubQuery?
        if (testNext(tkWhere)) {
            where = checkWhereSubQuery(line)
        } else {
            where = null
        }
        log.debug("found delete sql")
        addSqlStatement(DeleteStatement(line, tblName, where))
    }

    private fun selectStatement(line: Int) {
        next()
        val selectItems = mutableListOf<Token>()
        while (!scanner.eos() && currentToken().t != tkFrom) {
            val item = checkToken() // select子句暂时只支持单符号项，还不支持表达式比如 count(1), count(1) as c, age * 2
            selectItems += item
            if (!testNext(',')) {
                break
            }
        }
        checkNext(tkFrom)
        val froms = mutableListOf<String>()
        while (!scanner.eos() && currentToken().t == tkName) {
            val tblName = checkTableName()
            froms += tblName
            if (!testNext(',')) {
                break
            }
        }
        val joins = mutableListOf<JoinSubQuery>()
        while (!scanner.eos() && (currentToken().t == tkLeft || currentToken().t == tkRight
                        || currentToken().t == tkInner || currentToken().t == tkFull || currentToken().t == tkJoin)) {
            val joinType: String
            when (currentToken().t) {
                tkLeft -> {
                    joinType = "left"
                    next()
                }
                tkRight -> {
                    joinType = "right"
                    next()
                }
                tkInner -> {
                    joinType = "inner"
                    next()
                }
                tkFull -> {
                    joinType = "full"
                    next()
                }
                tkJoin -> {
                    joinType = "inner"
                }
                else -> {
                    throw SqlParseException("invalid join type ${currentToken()}")
                }
            }
            checkNext(tkJoin)
            val joinTargetTable = checkTableName()
            checkNext(tkOn)
            // 目前join xxx on a.b = c.d 子句中，on后表达式两个操作数都需要是a.b的格式
            val onSubQuery = checkOnSubQuery()
            joins += JoinSubQuery(joinType, joinTargetTable, onSubQuery)
        }
        val where = if (testNext(tkWhere)) checkWhereSubQuery(scanner.lineNumber) else null
        val orderBys = if (testNext(tkOrder) && testNext(tkBy)) checkOrderBySubQuery() else listOf()
        val groupBys = if (testNext(tkGroup) && testNext(tkBy)) checkGroupBySubQuery() else listOf()
        val limit = if (testNext(tkLimit)) checkLimitSubQuery() else null
        addSqlStatement(SelectStatement(line, selectItems, froms, joins, where, orderBys, groupBys, limit))
    }

    // 解析on后的子句
    private fun checkOnSubQuery(): OnSubQuery {
        val left = checkRefExprSubQuery()
        checkNext('=')
        val right = checkRefExprSubQuery()
        return OnSubQuery(left, right)
    }

    // 解析 a.b这类表达式
    private fun checkRefExprSubQuery(): RefExprSubQuery {
        val tblName = checkTableName()
        checkNext('.')
        val columnName = checkColumnName()
        return RefExprSubQuery(tblName, columnName)
    }

    // 解析order by后的子句
    private fun checkOrderBySubQuery(): List<OrderBySubQuery> {
        val items = mutableListOf<OrderBySubQuery>()
        while (!scanner.eos() && currentToken().t == tkName) {
            val colName = checkColumnName()
            val asc = if (currentToken().t == tkDesc) {
                next()
                false
            } else {
                true
            }
            items += OrderBySubQuery(colName, asc)
            if (!testNext(',')) {
                break
            }
        }
        return items
    }

    // 解析group by后的子句
    private fun checkGroupBySubQuery(): List<GroupBySubQuery> {
        val items = mutableListOf<GroupBySubQuery>()
        while (!scanner.eos() && currentToken().t == tkName) {
            val colName = checkColumnName()
            items += GroupBySubQuery(colName)
            if (!testNext(',')) {
                break
            }
        }
        return items
    }

    // 解析limit后的子句
    private fun checkLimitSubQuery(): LimitSubQuery {
        val val1 = checkToken()
        if (val1.t != tkInt) {
            throw SqlParseException("invalid limit syntax $val1")
        }
        if (testNext(',')) {
            val val2 = checkToken()
            if (val2.t != tkInt) {
                throw SqlParseException("invalid limit syntax $val2")
            }
            return LimitSubQuery(val1.i!!, val2.i!!)
        } else {
            return LimitSubQuery(0, val1.i!!)
        }
    }

    private fun alterStatement(line: Int) {
        next()
        checkNext(tkTable)
        val tblName = checkTableName()
        val alterActions = mutableListOf<AlterActionSubQuery>()
        while (!scanner.eos() && (currentToken().t == tkAdd || currentToken().t == tkDrop)) {
            val actionType = checkToken()
            if (actionType.t == tkDrop) {
                testNext(tkColumn)
            }
            val colName = checkColumnName()
            val colType: Token?
            when (actionType.t) {
                tkAdd -> {
                    colType = checkToken() // 目前字段类型只接受单符号类型，比如int, varchar, text, bool等
                }
                tkDrop -> {
                    colType = null
                }
                else -> {
                    throw SqlParseException("not supported alter action type $actionType")
                }
            }
            alterActions += AlterActionSubQuery(actionType, colName, colType)
            if (!testNext(',')) {
                break
            }
        }
        addSqlStatement(AlterStatement(line, tblName, alterActions))
    }

    private fun insertStatement(line: Int) {
        next()
        checkNext(tkInto)
        val tblName = checkTableName()
        checkNext('(')
        val columns = mutableListOf<String>()
        while (currentToken().t != ')'.toInt()) {
            val colName = checkName()
            columns += colName
            if (!testNext(',')) {
                break
            }
        }
        checkNext(')')
        checkNext(tkValues)
        val rows = mutableListOf<List<Token>>()
        while (currentToken().t == '('.toInt()) {
            checkNext('(')
            val values = mutableListOf<Token>()
            while (currentToken().t != ')'.toInt()) {
                val value = checkToken()
                values += value
                if (!testNext(',')) {
                    break
                }
            }
            checkNext(')')
            rows += values
            if (!testNext(',')) {
                break
            }
        }
        addSqlStatement(InsertStatement(line, tblName, columns, rows))
    }

    private fun checkColumnName(): String {
        return checkName()
    }

    private fun updateStatement(line: Int) {
        next()
        val tblName = checkTableName()
        checkNext(tkSet)
        val setItems = mutableListOf<Pair<String, Token>>()
        while (!scanner.eos() && currentToken().t == tkName) {
            val colName = checkColumnName()
            checkNext('=')
            // 目前update语句set字句后的右值只能是单符号表达式
            val value = checkToken()
            setItems += Pair(colName, value)
            if (!testNext(',')) {
                break
            }
        }
        val where = if (testNext(tkWhere)) {
            checkWhereSubQuery(line)
        } else {
            null
        }
        addSqlStatement(UpdateStatement(line, tblName, setItems, where))
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

    fun getStatements(): List<Node> {
        return parsedStatements
    }
}