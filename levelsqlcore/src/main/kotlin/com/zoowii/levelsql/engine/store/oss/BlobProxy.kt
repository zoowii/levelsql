package com.zoowii.levelsql.engine.store.oss

import java.io.OutputStream

interface BlobProxy {
    fun getContent(): ByteArray
    fun reload(): BlobProxy
    fun delete(): Boolean
    fun downloadTo(out: OutputStream)
}