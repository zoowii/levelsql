package com.zoowii.levelsql.sql.scanner

fun isSpace(t: Rune): Boolean {
    return t == ' '.toInt() || t == '\t'.toInt()
}

fun <T> popFromList(list: MutableList<T>): T {
    val value = list.last()
    list.removeAt(list.size-1)
    return value
}

fun <T> unshiftFromList(list: MutableList<T>): T {
    val value = list[0]
    list.removeAt(0)
    return value
}