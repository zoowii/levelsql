package com.zoowii.levelsql.engine.utils

fun compareBytes(a: ByteArray, b: ByteArray): Int {
    if(a.size!=b.size) {
        return a.size - b.size
    }
    for(i in a.indices) {
        if(a[i] != b[i]) {
            return a[i] - b[i]
        }
    }
    return 0
}


fun compareNodeKey(a: ByteArray, b: ByteArray): Int {
    return compareBytes(a, b)
}
