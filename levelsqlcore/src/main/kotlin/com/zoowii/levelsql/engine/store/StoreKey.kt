package com.zoowii.levelsql.engine.store

/**
 * IStore操作存储模块的key的结构
 * namespace是区分不同命名空间的区分词，比如不同索引，不同表有不同的namespace
 * seq是一个顺序增加的整数，一般每次只增加1(或者比较少的数量)
 */
data class StoreKey(val namespace: String, val seq: Long) {
    override fun toString(): String {
        return "store:$namespace:$seq"
    }
    fun toBytes(): ByteArray {
        return this.toString().toByteArray(java.nio.charset.Charset.forName("UTF-8"))
    }

    fun getNonNegativeSeq(): Long {
        return seq + 1 // 因为seq可能是-1(nodeId=-1时表示根节点)
    }
}