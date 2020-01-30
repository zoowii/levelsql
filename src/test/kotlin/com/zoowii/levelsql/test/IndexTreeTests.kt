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

    private var _tree: IndexTree? = null
    private val lastRowIdGen: AtomicLong = AtomicLong(0)

    private fun nextRowId(): RowId {
        return RowId.fromLong(lastRowIdGen.getAndIncrement())
    }

    @Before fun initEmptyTree() {
        val dbFile = File("./test_db")
        if(dbFile.exists())
            dbFile.deleteRecursively()
        val store = LevelDbStore.openStore(dbFile)

        _tree = IndexTree(store, "test_index_name", 1024 * 16, 4, true)
        _tree?.initTree()
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
    private fun createSimpleTree() {
        val tree = _tree ?: return
        val keysCount = simpleTreeKeysCount
        for(i in 1 until keysCount) {
            tree.addKeyValue(IndexLeafNodeValue(nextRowId(), i.toBytes(), i.toBytes()))
        }
    }

    private fun createReverseSimpleTree() {
        val tree = _tree ?: return
        val keysCount = simpleTreeKeysCount
        for(i in (keysCount-1) downTo 0) {
            val insertValue = i
            tree.addKeyValue(IndexLeafNodeValue(nextRowId(), insertValue.toBytes(), insertValue.toBytes()))
        }
//        tree.addKeyValue(IndexLeafNodeValue(5.toBytes()))
    }

    @Test fun testInsertReverse() {
        val tree = _tree ?: return
        createReverseSimpleTree()
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
        val tree = _tree ?: return
        createSimpleTree()
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
        val tree = _tree ?: return
        createSimpleTree()
        val keysCount = simpleTreeKeysCount
        // 插入一遍重复key，但是value不一样
        for(i in 1 until keysCount) {
            tree.addKeyValue(IndexLeafNodeValue(nextRowId(), i.toBytes(), (i+100).toBytes()))
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
        createSimpleTree()
        val tree = _tree ?: return

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
        createSimpleTree()
        val tree = _tree ?: return
        val keysCount = simpleTreeKeysCount
        // 插入一遍重复key，但是value不一样
        for(i in 1 until keysCount) {
            tree.addKeyValue(IndexLeafNodeValue(nextRowId(), i.toBytes(), (i+100).toBytes()))
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
        createSimpleTree()
        val tree = _tree ?: return
        val keysCount = simpleTreeKeysCount
        // 插入一遍重复key，但是value不一样
        for(i in 1 until keysCount) {
            tree.addKeyValue(IndexLeafNodeValue(nextRowId(), i.toBytes(), (i+100).toBytes()))
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
        createSimpleTree()
        val tree = _tree ?: return
        val keysCount = simpleTreeKeysCount
        // 插入一遍重复key，但是value不一样
        for(i in 1 until keysCount) {
            tree.addKeyValue(IndexLeafNodeValue(nextRowId(), i.toBytes(), (i+100).toBytes()))
        }

        val treeJson = tree.toFullTreeString()
//        println("tree:")
//        println(treeJson)
        writeTreeJsonToFile(treeJson, "test_docs/testSeekByGreatCondition.json")
        for(i in 1 until keysCount-1) {
            val cond = GreatThanKeyCondition(i.toBytes())
            if(i==11) {
                print("")
            }
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