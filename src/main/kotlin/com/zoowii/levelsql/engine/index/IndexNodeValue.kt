package com.zoowii.levelsql.engine.index

data class IndexNodeValue(val node: IndexNode, val indexInNode: Int) {
    fun leafRecord(): IndexLeafNodeValue {
        return node.values[indexInNode]
    }
}
