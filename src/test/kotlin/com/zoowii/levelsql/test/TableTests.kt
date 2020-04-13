package com.zoowii.levelsql.test

import com.zoowii.levelsql.engine.*
import com.zoowii.levelsql.engine.store.LocalFileStore
import com.zoowii.levelsql.engine.store.toBytes
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import com.zoowii.levelsql.engine.types.Row
import com.zoowii.levelsql.engine.utils.*
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.File
import kotlin.test.assertTrue

class TableTests {
    // private var db: Database? = null

    // @Before
    fun initDb() {
//        val dbFile = File("./table_tests")
//        if (dbFile.exists())
//            dbFile.deleteRecursively()
//        val store = LevelDbStore.openStore(dbFile)

        val localDbFile = File("./table_tests_local")
        // 为了测试readDb，不删除旧数据文件
//        if (localDbFile.exists())
//            localDbFile.deleteRecursively()
        val localStore = LocalFileStore.openStore(localDbFile)

        val db = Database("test", localStore)
    }

    private fun cleanAndInitDb(testCaseName: String, clean: Boolean = true): Database {
        val localDbFile = File("./table_tests_local/" + testCaseName)
        if (clean && localDbFile.exists())
            localDbFile.deleteRecursively()
        val localStore = LocalFileStore.openStore(localDbFile)

        val db = Database("test", localStore)
        return db
    }

    // @After
    fun closeDb(db: Database) {
        if(db!=null) {
            db.store.close()
        }
    }

    private val userTableName = "user"

    private fun openSimpleTables(db: Database): Table {
        val columns = listOf(
                TableColumnDefinition("id", IntColumnType(), true),
                TableColumnDefinition("name", VarCharColumnType(50), true),
                TableColumnDefinition("age", IntColumnType(), true)
        )
        return Table(db, userTableName, "id", columns, 1024 * 16, 4)
    }

    private fun writeTreeJsonToFile(treeJsonStr: String, outputPath: String) {
        val file = File(outputPath)
        if (!file.exists()) {
            file.parentFile.mkdirs()
            file.createNewFile()
        }
        file.writeText(treeJsonStr)
    }

    @Test
    fun testReadDb() {
        var db = cleanAndInitDb("testReadDb", true)

        try {
            run {
                val table = openSimpleTables(db)
                val rowsCount = 10
                // 插入2遍
                for (i in 1..rowsCount) {
                    val k = intDatum(i)
                    val record = singleStringRow("hello_$i")
                    table.rawInsert(null, k, record)
                }
                for (i in 1..rowsCount) {
                    val k = intDatum(i)
                    val record = singleStringRow("hello_$i")
                    table.rawInsert(null, k, record)
                }
                closeDb(db)
            }

            db = cleanAndInitDb("testReadDb", false) // close store and reopen db

            val table = openSimpleTables(db)
            val treeJson = table.toFullTreeString()
            println("tree:")
            println(treeJson)
            writeTreeJsonToFile(treeJson, "test_table_docs/testReadDb.json")
            val rowsCount = 10
            // test inserted rows
            for (i in 1..rowsCount) {
                val k = intDatum(i).toBytes()
                val records = table.rawFind(null, EqualKeyCondition(k))
                assert(records.size == 2)
                for (r in records) {
                    assert(compareNodeKey(r.key, k) == 0)
                }
            }
            assertTrue(table.validate(), "invalid tree")
        } finally {
            closeDb(db)
        }
    }

    private fun singleStringRow(str: String): Row {
        val row = Row()
        row.data = listOf(Datum(DatumTypes.kindString, stringValue = str))
        return row
    }

    private fun bytesToSingleStringRow(data: ByteArray): Row {
        val row = Row()
        row.fromBytes(ByteArrayStream(data))
        return row
    }

    private fun intDatum(n: Int): Datum {
        return Datum(DatumTypes.kindInt64, intValue = n.toLong())
    }

    private fun bytesToIntDatum(data: ByteArray): Datum {
        val d = Datum(DatumTypes.kindNull)
        d.fromBytes(ByteArrayStream(data))
        return d
    }

    @Test
    fun testInsertRows() {
        val db = cleanAndInitDb("testInsertRows", true)
        try {
            val table = openSimpleTables(db)
            val rowsCount = 10
            // 插入2遍
            for (i in 1..rowsCount) {
                val k = intDatum(i)
                val record = singleStringRow("hello_$i")
                table.rawInsert(null, k, record)
            }
            for (i in 1..rowsCount) {
                val k = intDatum(i)
                val record = singleStringRow("hello_$i")
                table.rawInsert(null, k, record)
            }
            val treeJson = table.toFullTreeString()
            println("tree:")
            println(treeJson)
            writeTreeJsonToFile(treeJson, "test_table_docs/testInsertRows.json")
            // test inserted rows
            for (i in 1..rowsCount) {
                val k = intDatum(i).toBytes()
                val records = table.rawFind(null, EqualKeyCondition(k))
                println("key " + i + " records count " + records.size)
                assert(records.size == 2)
                for (r in records) {
                    assert(compareNodeKey(r.key, k) == 0)
                }
            }
            assertTrue(table.validate(), "invalid tree")
        } finally {
            closeDb(db)
        }
    }

    @Test
    fun testUpdateRow() {
        val db = cleanAndInitDb("testUpdateRow", true)
        try {
            val table = openSimpleTables(db)
            val rowsCount = 10
            for (i in 1..rowsCount) {
                val k = intDatum(i)
                val record = singleStringRow("hello_$i")
                table.rawInsert(null, k, record)
            }
            val toUpdateKey = 3
            val cond = EqualKeyCondition(intDatum(toUpdateKey).toBytes())
            val foundRows = table.rawFind(null, cond)
            assert(foundRows.isNotEmpty())
            val row = foundRows[0]
            val newRecord = singleStringRow("world_$toUpdateKey")
            table.rawUpdate(null, row.rowId, bytesToIntDatum(row.key), newRecord)
            val rowsAfterUpdate = table.rawFind(null, cond)
            assert(rowsAfterUpdate.isNotEmpty())
            val rowUpdated = rowsAfterUpdate[0]
            assertTrue(rowUpdated.rowId == row.rowId
                    && compareNodeKey(rowUpdated.key, row.key) == 0
                    && compareBytes(rowUpdated.value, newRecord.toBytes()) == 0)
            assertTrue(table.validate(), "invalid tree")
        } finally {
            closeDb(db)
        }
    }

    @Test
    fun testDeleteRow() {
        val db = cleanAndInitDb("testDeleteRow", true)
        try {
            val table = openSimpleTables(db)
            val rowsCount = 10
            for (i in 1..rowsCount) {
                val k = intDatum(i)
                val record = singleStringRow("hello_$i")
                table.rawInsert(null, k, record)
            }
            val toDeleteKey = 3
            val cond = EqualKeyCondition(intDatum(toDeleteKey).toBytes())
            val foundRows = table.rawFind(null, cond)
            assert(foundRows.isNotEmpty())
            val row = foundRows[0]
            table.rawDelete(null, bytesToIntDatum(row.key), row.rowId)

            val rowsAfterUpdate = table.rawFind(null, cond)
            assert(rowsAfterUpdate.isEmpty())
            assertTrue(table.validate(), "invalid tree")
        } finally {
            closeDb(db)
        }
    }

    @Test
    fun testSelectRow() {
        val db = cleanAndInitDb("testSelectRow", true)
        try {
            val table = openSimpleTables(db)
            val rowsCount = 10
            // 插入2遍
            for (i in 1..rowsCount) {
                val k = intDatum(i)
                val record = singleStringRow("hello_$i")
                table.rawInsert(null, k, record)
            }
            for (i in 1..rowsCount) {
                val k = intDatum(i)
                val record = singleStringRow("hello_$i")
                table.rawInsert(null, k, record)
            }
            // test select by condition
            for (i in 1..rowsCount) {
                val k = intDatum(i).toBytes()
                val records = table.rawFind(null, GreatThanKeyCondition(k))
                println("records count ${records.size}")
                assert(records.size == (2 * (rowsCount - i)))
                if (records.isNotEmpty()) {
                    val r = records[0]
                    assert(compareNodeKey(r.key, intDatum(i + 1).toBytes()) == 0)
                }
            }
            assertTrue(table.validate(), "invalid tree")
        } finally {
            closeDb(db)
        }
    }

    @Test
    fun testProjection() {
        // TODO
    }

    @Test
    fun testJoinTable() {
        // TODO
    }

    @Test
    fun testOrderBy() {
        // TODO
    }
}