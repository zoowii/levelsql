package com.zoowii.levelsql.test

import com.zoowii.levelsql.IntColumnType
import com.zoowii.levelsql.TableColumnDefinition
import com.zoowii.levelsql.VarCharColumnType
import com.zoowii.levelsql.engine.Database
import com.zoowii.levelsql.engine.Table
import com.zoowii.levelsql.engine.store.LevelDbStore
import com.zoowii.levelsql.engine.store.LocalFileStore
import com.zoowii.levelsql.engine.store.toBytes
import com.zoowii.levelsql.engine.utils.EqualKeyCondition
import com.zoowii.levelsql.engine.utils.GreatThanKeyCondition
import com.zoowii.levelsql.engine.utils.compareBytes
import com.zoowii.levelsql.engine.utils.compareNodeKey
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.io.File
import kotlin.test.assertTrue

class TableTests {
    private var db: Database? = null

    @Before
    fun iniDb() {
//        val dbFile = File("./table_tests")
//        if (dbFile.exists())
//            dbFile.deleteRecursively()
//        val store = LevelDbStore.openStore(dbFile)

        // LocalFileStore需要更充分的测试
        val localDbFile = File("./table_tests_local")
        // 为了测试readDb，不删除旧数据文件
//        if (localDbFile.exists())
//            localDbFile.deleteRecursively()
        val localStore = LocalFileStore.openStore(localDbFile)

        db = Database("test", localStore)
    }

    @After
    fun closeDb() {
        if(db!=null) {
            db!!.store.close()
        }
    }

    private val userTableName = "user"

    private fun openSimpleTables(): Table {
        val columns = listOf(
                TableColumnDefinition("id", IntColumnType(), true),
                TableColumnDefinition("name", VarCharColumnType(50), true),
                TableColumnDefinition("age", IntColumnType(), true)
        )
        return Table(db!!, userTableName, columns, 1024 * 16, 4)
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
        val table = openSimpleTables()
        val treeJson = table.toFullTreeString()
        println("tree:")
        println(treeJson)
        writeTreeJsonToFile(treeJson, "test_table_docs/testReadDb.json")
        val rowsCount = 10
        // test inserted rows
        for (i in 1..rowsCount) {
            val k = i.toBytes()
            val records = table.rawFind(EqualKeyCondition(k))
            assert(records.size == 2)
            for (r in records) {
                assert(compareNodeKey(r.key, k) == 0)
            }
        }
        assertTrue(table.validate(), "invalid tree")
    }

    @Test
    fun testInsertRows() {
        val table = openSimpleTables()
        val rowsCount = 10
        // 插入2遍
        for (i in 1..rowsCount) {
            val k = i.toBytes()
            val record = "hello_$i".toBytes()
            table.rawInsert(k, record)
        }
        for (i in 1..rowsCount) {
            val k = i.toBytes()
            val record = "hello_$i".toBytes()
            table.rawInsert(k, record)
        }
        val treeJson = table.toFullTreeString()
        println("tree:")
        println(treeJson)
        writeTreeJsonToFile(treeJson, "test_table_docs/testInsertRows.json")
        // test inserted rows
        for (i in 1..rowsCount) {
            val k = i.toBytes()
            val records = table.rawFind(EqualKeyCondition(k))
            assert(records.size == 2)
            for (r in records) {
                assert(compareNodeKey(r.key, k) == 0)
            }
        }
        assertTrue(table.validate(), "invalid tree")
    }

    @Test
    fun testUpdateRow() {
        val table = openSimpleTables()
        val rowsCount = 10
        for (i in 1..rowsCount) {
            val k = i.toBytes()
            val record = "hello_$i".toBytes()
            table.rawInsert(k, record)
        }
        val toUpdateKey = 3
        val cond = EqualKeyCondition(toUpdateKey.toBytes())
        val foundRows = table.rawFind(cond)
        assert(foundRows.isNotEmpty())
        val row = foundRows[0]
        val newRecordBytes = "world_$toUpdateKey".toBytes()
        table.rawUpdate(row.rowId, row.key, newRecordBytes)
        val rowsAfterUpdate = table.rawFind(cond)
        assert(rowsAfterUpdate.isNotEmpty())
        val rowUpdated = rowsAfterUpdate[0]
        assertTrue(rowUpdated.rowId == row.rowId
                && compareNodeKey(rowUpdated.key, row.key) == 0
                && compareBytes(rowUpdated.value, newRecordBytes) == 0)
        assertTrue(table.validate(), "invalid tree")
    }

    @Test
    fun testDeleteRow() {
        val table = openSimpleTables()
        val rowsCount = 10
        for (i in 1..rowsCount) {
            val k = i.toBytes()
            val record = "hello_$i".toBytes()
            table.rawInsert(k, record)
        }
        val toDeleteKey = 3
        val cond = EqualKeyCondition(toDeleteKey.toBytes())
        val foundRows = table.rawFind(cond)
        assert(foundRows.isNotEmpty())
        val row = foundRows[0]
        table.rawDelete(row.key, row.rowId)

        val rowsAfterUpdate = table.rawFind(cond)
        assert(rowsAfterUpdate.isEmpty())
        assertTrue(table.validate(), "invalid tree")
    }

    @Test
    fun testSelectRow() {
        val table = openSimpleTables()
        val rowsCount = 10
        // 插入2遍
        for (i in 1..rowsCount) {
            val k = i.toBytes()
            val record = "hello_$i".toBytes()
            table.rawInsert(k, record)
        }
        for (i in 1..rowsCount) {
            val k = i.toBytes()
            val record = "hello_$i".toBytes()
            table.rawInsert(k, record)
        }
        // test select by condition
        for (i in 1..rowsCount) {
            val k = i.toBytes()
            val records = table.rawFind(GreatThanKeyCondition(k))
            println("records count ${records.size}")
            assert(records.size == (2 * (rowsCount - i)))
            if (records.isNotEmpty()) {
                val r = records[0]
                assert(compareNodeKey(r.key, (i + 1).toBytes()) == 0)
            }
        }
        assertTrue(table.validate(), "invalid tree")
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