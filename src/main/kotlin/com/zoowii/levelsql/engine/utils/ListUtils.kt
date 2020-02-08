package com.zoowii.levelsql.engine.utils

import java.util.*

fun <E> List<E>.removeIndex(idx: Int): List<E> {
    if(isEmpty())
        return this
    val len = size
    val maxIdx = len - 1
    if(idx > maxIdx || idx < 0)
        return this
    if(idx == 0)
        return this.subList(1, len)
    if(idx == maxIdx)
        return this.subList(0, maxIdx)
    return this.subList(0, idx) + this.subList(idx+1, len)
}

fun <E> List<E>.safeSlice(start: Int, end: Int): List<E> {
    var realEnd = end
    if(realEnd<0)
        realEnd = this.size + end
    if(realEnd <= start) {
        return emptyList()
    }
    if(start >= this.size) {
        return emptyList()
    }
    if(realEnd > this.size) {
        realEnd = this.size
    }
    return this.subList(start, realEnd)
}

fun ByteArray.safeSlice(start: Int, end: Int): ByteArray {
    var realEnd = end
    if(realEnd<0)
        realEnd = this.size + end
    if(realEnd <= start) {
        return byteArrayOf()
    }
    if(start >= this.size) {
        return byteArrayOf()
    }
    if(realEnd > this.size) {
        realEnd = this.size
    }
    val result = ByteArray(realEnd - start)
    (start until realEnd).mapIndexed { newIndex, originIndex ->
        result[newIndex] = this[originIndex]
    }
    return result
}

// 当byte array长度不够{targetSize}时，左侧补0字节
fun ByteArray.leftFillZero(targetSize: Int): ByteArray {
    if(this.size>=targetSize) {
        return this
    }
    val result = ByteArray(targetSize)
    (0 until (targetSize - size)).map {
        result[it] = 0
    }
    ((targetSize - size) until targetSize).map {
        result[it] = this[it - targetSize + size]
    }
    return result
}