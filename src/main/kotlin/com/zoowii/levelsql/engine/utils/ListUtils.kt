package com.zoowii.levelsql.engine.utils

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
        realEnd = end
    }
    return this.subList(start, realEnd)
}