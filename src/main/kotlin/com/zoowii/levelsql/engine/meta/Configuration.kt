package com.zoowii.levelsql.engine.meta

import kotlin.math.ceil
import kotlin.math.floor

/**
 * @param keyUnique whether same key can have multiple values
 */
data class BPlusTreeConfiguration(var innerNodeDegree: Int, var leafNodeDegree: Int, var keyUnique: Boolean) {
    companion object {
        fun calculateLeafNodeDegree(leafNodeMaxBytesCount: Int, entrySize: Int): Int {
            return leafNodeMaxBytesCount / entrySize
        }
    }
}