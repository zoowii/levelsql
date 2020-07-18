package com.zoowii.levelsql.engine.index

import com.zoowii.levelsql.engine.exceptions.IndexException
import com.zoowii.levelsql.engine.types.Datum
import com.zoowii.levelsql.engine.types.DatumTypes
import java.io.ByteArrayOutputStream

public typealias IndexKey = ByteArray

// 把若干个datum转成索引的key. 单字段索引只传一个值，联合索引按顺序传索引的各列的值
// 如果datum是字符串等不定长度的值，需要补齐或者裁减为固定长度字节数组
// 如果是多个datum，则在分别裁减补齐转成字节数组后按顺序拼接，从而可以用于联合索引的查询
fun datumsToIndexKey(datums: List<Datum>): IndexKey {
    if(datums.isEmpty()) {
        throw IndexException("can't transform empty datums to index key")
    }
    fun datumToIndexKey(datum: Datum): ByteArray {
        return datum.toBytes(true)
    }
    val indexKey = ByteArrayOutputStream()
    for(datum in datums) {
        indexKey.write(datumToIndexKey(datum))
    }
    return indexKey.toByteArray()
}

fun datumsToIndexKey(datum: Datum): IndexKey {
    return datumsToIndexKey(listOf(datum))
}