package com.zoowii.levelsql.protocol.mysql.field

// https://dev.mysql.com/doc/internals/en/com-query-response.html#text-resultset
object FieldTypes {
    val DECIMAL = 0
    val TINY = 1
    val SHORT = 2
    val LONG = 3
    val FLOAT = 4
    val DOUBLE = 5
    val NULL = 6
    val TIMESTAMP = 7
    val LONGLONG = 8
    val INT24 = 9
    val DATE = 10
    val TIME = 11
    val DATETIME = 12
    val YEAR = 13
    val NEWDATE = 14
    val VARCHAR = 15
    val BIT = 16
    val JSON = 245
    val NEWDECIMAL = 246
    val ENUM = 247
    val SET = 248
    val TINY_BLOB = 249
    val MEDIUM_BLOB = 250
    val LONG_BLOB = 251
    val BLOB = 252
    val VAR_STRING = 253
    val STRING = 254
    val GEOMETRY = 255
}