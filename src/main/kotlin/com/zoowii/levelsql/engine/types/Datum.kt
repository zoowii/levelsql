package com.zoowii.levelsql.engine.types

typealias DatumType = Int

object DatumTypes {
    val kindNull: DatumType = 0
    val kindInt64: DatumType = 1
    val kindString: DatumType = 2
    val kindText: DatumType = 3
    val kindBool: DatumType = 4
}

class Datum(val kind: DatumType) {
    var intValue: Long? = null
    var stringValue: String? = null
    var boolValue: Boolean? = null
}