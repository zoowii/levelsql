package com.zoowii.levelsql.test

import com.zoowii.levelsql.engine.index.IndexLeafNodeValue
import com.zoowii.levelsql.engine.index.IndexTree
import com.zoowii.levelsql.engine.store.RowId
import com.zoowii.levelsql.engine.store.LevelDbStore
import com.zoowii.levelsql.engine.store.bytesToHex
import com.zoowii.levelsql.engine.store.toBytes
import com.zoowii.levelsql.engine.utils.EqualKeyCondition
import com.zoowii.levelsql.engine.utils.GreatThanKeyCondition
import com.zoowii.levelsql.engine.utils.LessThanKeyCondition
import org.junit.Before
import org.junit.Test
import java.io.File
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertTrue

class IndexTreeTestCase {

    class TestSession(val sessionName: String) {
        var tree: IndexTree? = null

        private val lastRowIdGen: AtomicLong = AtomicLong(0)
        fun nextRowId(): RowId {
            return RowId.fromLong(lastRowIdGen.getAndIncrement())
        }
    }

    private fun cleanAndInitEmptyTree(sess: TestSession) {
        val dbFile = File("./test_db/" + sess.sessionName)
        if(dbFile.exists())
            dbFile.deleteRecursively()
        val store = LevelDbStore.openStore(dbFile)

        sess.tree = IndexTree(store, "test_index_name", 1024 * 16, 4, true)
        sess.tree?.initTree()
    }

    private fun writeTreeJsonToFile(treeJsonStr: String, outputPath: String) {
        val file = File(outputPath)
        if(!file.exists()) {
            file.parentFile.mkdirs()
            file.createNewFile()
        }
        file.writeText(treeJsonStr)
    }

    private val simpleTreeKeysCount = 16 // 8, 16
    private fun createSimpleTree(sess: TestSession) {
        val tree = sess.tree ?: return
        val keysCount = simpleTreeKeysCount
        for(i in 1 until keysCount) {
            tree.addKeyValue(IndexLeafNodeValue(sess.nextRowId(), i.toBytes(), i.toBytes()))
        }
    }

    private fun createReverseSimpleTree(sess: TestSession) {
        val tree = sess.tree ?: return
        val keysCount = simpleTreeKeysCount
        for(i in (keysCount-1) downTo 0) {
            val insertValue = i
            tree.addKeyValue(IndexLeafNodeValue(sess.nextRowId(), insertValue.toBytes(), insertValue.toBytes()))
        }
//        tree.addKeyValue(IndexLeafNodeValue(5.toBytes()))
    }

    @Test fun testInsertReverse() {
        val sess = TestSession("testInsertReverse")
        cleanAndInitEmptyTree(sess)
        val tree = sess.tree ?: return
        createReverseSimpleTree(sess)
        val keysCount = simpleTreeKeysCount
        val treeJson = tree.toFullTreeString()
        println("Reverse tree:")
        println(treeJson)
        writeTreeJsonToFile(treeJson, "test_docs/testInsertReverse.json")
        // test tree structure after insert
        for(i in 1 until keysCount+1) {
            val insertValue = i
            val valueNode = tree.findIndex(insertValue.toBytes())
            println(insertValue.toString() + " " + valueNode.toString())
            if(i==keysCount)
                assertTrue(valueNode.first == null)
            else
                assertTrue(valueNode.first != null)
        }
        assertTrue(tree.validate(), "invalid tree")
    }

    @Test fun testInsert() {
        val sess = TestSession("testInsert")
        cleanAndInitEmptyTree(sess)
        val tree = sess.tree ?: return
        createSimpleTree(sess)
        val keysCount = simpleTreeKeysCount
        val treeJson = tree.toFullTreeString()
        println("tree:")
        println(treeJson)
        writeTreeJsonToFile(treeJson, "test_docs/testInsert.json")
        // test tree structure after insert
        for(i in 1 until keysCount+1) {
            val valueNode = tree.findIndex(i.toBytes())
            println(i.toString() + " " + valueNode.toString())
            if(i==keysCount)
                assertTrue(valueNode.first == null)
            else
                assertTrue(valueNode.first != null)
        }
        assertTrue(tree.validate(), "invalid tree")
    }

    @Test fun testInsertDuplicate() {
        val sess = TestSession("testInsertDuplicate")
        cleanAndInitEmptyTree(sess)
        val tree = sess.tree ?: return
        createSimpleTree(sess)
        val keysCount = simpleTreeKeysCount
        // 插入一遍重复key，但是value不一样
        for(i in 1 until keysCount) {
            tree.addKeyValue(IndexLeafNodeValue(sess.nextRowId(), i.toBytes(), (i+100).toBytes()))
        }

        val treeJson = tree.toFullTreeString()
        println("tree:")
        println(treeJson)
        writeTreeJsonToFile(treeJson, "test_docs/testInsertDuplicate.json")
        // test tree structure after insert
        for(i in 1 until keysCount+1) {
            val valueNode = tree.findIndex(i.toBytes())
            println(i.toString() + " " + valueNode.toString())
            if(i==keysCount)
                assertTrue(valueNode.first == null)
            else
                assertTrue(valueNode.first != null)
        }
        assertTrue(tree.validate(), "invalid tree")
    }

    @Test fun testDelete() {
        val sess = TestSession("testDelete")
        cleanAndInitEmptyTree(sess)
        createSimpleTree(sess)
        val tree = sess.tree ?: return

        val item1 = 9
        val item1Key = item1.toBytes()
        val (nodeAndPos, isNew) = tree.findIndex(item1Key, false)
        assertTrue(!isNew)
        val item1RowId = nodeAndPos!!.node.values[nodeAndPos.indexInNode].rowId
        tree.deleteByKeyAndRowId(item1Key, item1RowId)
        assertTrue(tree.validate(), "tree invalid after deleted key $item1")
        val treeJson = tree.toFullTreeString()
        println("tree after delete:")
        println(treeJson)
        writeTreeJsonToFile(treeJson, "test_docs/testDelete.json")
    }

    @Test fun testQuery() {

    }

    @Test fun testSeekByEqualCondition() {
        // 测试按=条件seek查找
        val sess = TestSession("testSeekByEqualCondition")
        cleanAndInitEmptyTree(sess)
        createSimpleTree(sess)
        val tree = sess.tree ?: return
        val keysCount = simpleTreeKeysCount
        // 插入一遍重复key，但是value不一样
        for(i in 1 until keysCount) {
            tree.addKeyValue(IndexLeafNodeValue(sess.nextRowId(), i.toBytes(), (i+100).toBytes()))
        }

        val treeJson = tree.toFullTreeString()
        println("tree:")
        println(treeJson)
        writeTreeJsonToFile(treeJson, "test_docs/testSeekByEqualCondition.json")
        for(i in 1 until keysCount) {
            val cond = EqualKeyCondition(i.toBytes())
            val found = tree.seekByCondition(cond)
            assertTrue(found!=null)
            if(found==null) {
                continue
            }
            println("${i} item ${bytesToHex(found.leafRecord().value)}")
            // 需要assert找到的项是第一个满足要求的项，左侧record不满足要求
            assertTrue(cond.match(found.leafRecord().key))
            val leftRecord = tree.prevRecordPosition(found)
            assert(leftRecord==null || !cond.match(leftRecord.leafRecord().key))
        }
    }

    @Test fun testSeekByLessCondition() {
        // 测试按 < 条件seek查找
        val sess = TestSession("testSeekByLessCondition")
        cleanAndInitEmptyTree(sess)
        createSimpleTree(sess)
        val tree = sess.tree ?: return
        val keysCount = simpleTreeKeysCount
        // 插入一遍重复key，但是value不一样
        for(i in 1 until keysCount) {
            tree.addKeyValue(IndexLeafNodeValue(sess.nextRowId(), i.toBytes(), (i+100).toBytes()))
        }

        val treeJson = tree.toFullTreeString()
        println("tree:")
        println(treeJson)
        writeTreeJsonToFile(treeJson, "test_docs/testSeekByLessCondition.json")
        for(i in 2 until keysCount) {
            val cond = LessThanKeyCondition(i.toBytes())
            val found = tree.seekByCondition(cond)
            assertTrue(found!=null)
            if(found==null) {
                continue
            }
            println("< ${i} item: ${bytesToHex(found.leafRecord().value)}")
            // 需要assert找到的项是第一个满足要求的项，左侧record不满足要求
            assertTrue(cond.match(found.leafRecord().key))
            val leftRecord = tree.prevRecordPosition(found)
            if(i==3) {
                print("")
            }
            assert(leftRecord==null || !cond.match(leftRecord.leafRecord().key))
        }
    }

    @Test fun testSeekByGreatCondition() {
        // 测试按 > 条件seek查找
        val sess = TestSession("testSeekByGreatCondition")
        cleanAndInitEmptyTree(sess)
        createSimpleTree(sess)
        val tree = sess.tree ?: return
        val keysCount = simpleTreeKeysCount
        // 插入一遍重复key，但是value不一样
        for(i in 1 until keysCount) {
            tree.addKeyValue(IndexLeafNodeValue(sess.nextRowId(), i.toBytes(), (i+100).toBytes()))
        }

        val treeJson = tree.toFullTreeString()
//        println("tree:")
//        println(treeJson)
        writeTreeJsonToFile(treeJson, "test_docs/testSeekByGreatCondition.json")
        for(i in 1 until keysCount-1) {
            val cond = GreatThanKeyCondition(i.toBytes())
            val found = tree.seekByCondition(cond)
            assertTrue(found!=null)
            if(found==null) {
                continue
            }
            println("> ${i} item: ${bytesToHex(found.leafRecord().value)}")
            // 需要assert找到的项是第一个满足要求的项，左侧record不满足要求
            assertTrue(cond.match(found.leafRecord().key))
            val leftRecord = tree.prevRecordPosition(found)
            assert(leftRecord==null || !cond.match(leftRecord.leafRecord().key))
        }
    }

}