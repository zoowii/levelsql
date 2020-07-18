package com.zoowii.levelsql.engine.types

class SqlResultSet {
    var columns: List<String> = listOf()
    var chunk: Chunk = Chunk()

    override fun toString(): String {
        return "${columns.joinToString("\t")}\n${chunk.rows.joinToString("\n")}"
    }
}