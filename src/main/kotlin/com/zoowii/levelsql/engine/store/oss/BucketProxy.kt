package com.zoowii.levelsql.engine.store.oss

interface BucketProxy {
    fun get(blobName: String): BlobProxy?
    fun create(blobName: String, content: ByteArray, contentType: String): BlobProxy
}