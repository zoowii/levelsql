package com.zoowii.levelsql.engine.types

// 多行数据构成的数据包
class Chunk() {
    var rows = mutableListOf<Row>()

    fun replaceRows(rows: List<Row>): Chunk {
        this.rows.clear()
        this.rows.addAll(rows)
        return this
    }

    companion object {
        fun mergeChunks(list: List<Chunk>): Chunk {
            val result = Chunk()
            for(item in list) {
                result.rows.addAll(item.rows)
            }
            return result
        }
    }
}