package com.zoowii.levelsql.engine.types

// 多行数据构成的数据包
class Chunk {
    var rows = mutableListOf<Row>()
}