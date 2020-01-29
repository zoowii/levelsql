package com.zoowii.levelsql.sql.scanner

fun isSpace(t: Rune): Boolean {
    return t == ' '.toInt() || t == '\t'.toInt()
}