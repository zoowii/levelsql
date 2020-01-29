package com.zoowii.levelsql.engine.sql.scanner

fun isSpace(t: Rune): Boolean {
    return t == ' '.toInt() || t == '\t'.toInt()
}