package com.zoowii.levelsql.engine.types

// 多行数据构成的数据包
class Chunk() {
    var rows = mutableListOf<Row>()

    fun replaceRows(rows: List<Row>): Chunk {
        this.rows.clear()
        this.rows.addAll(rows)
        return this
    }

    override fun toString(): String {
        return "Chunk(rows=$rows)"
    }

    companion object {
        fun mergeChunks(list: List<Chunk>): Chunk {
            val result = Chunk()
            for(item in list) {
                result.rows.addAll(item.rows)
            }
            return result
        }

        fun singleLongValue(value: Long): Chunk {
            val row = Row()
            row.data = listOf(Datum(DatumTypes.kindInt64, intValue = value))
            return Chunk().replaceRows(listOf(row))
        }
    }


}