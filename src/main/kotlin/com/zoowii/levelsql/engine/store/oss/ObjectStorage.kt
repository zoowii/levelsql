package com.zoowii.levelsql.engine.store.oss

interface ObjectStorage {
    fun createBucket(name: String): BucketProxy
    fun getBucket(name: String): BucketProxy
}