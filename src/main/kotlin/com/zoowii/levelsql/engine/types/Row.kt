package com.zoowii.levelsql.engine.types

class Row {
    val data = mutableListOf<Datum>()

    override fun toString(): String {
        return data.joinToString(",\t")
    }
}