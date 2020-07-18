package com.zoowii.levelsql.engine.store

import com.zoowii.levelsql.engine.store.oss.BucketProxy
import com.zoowii.levelsql.engine.store.oss.ObjectStorage

// OSS类产品的Store接口包装
class OssStore(val storage: ObjectStorage, val bucket: BucketProxy) : IStore {
    private val binaryMimeType = "application/oct-stream"
    override fun put(key: StoreKey, value: ByteArray) {
        val blobName = key.toString()
        var blob = bucket.get(blobName)
        if(blob == null) {
            blob = bucket.create(blobName, value, binaryMimeType)
        } else {
            // !!! slow and dangerous
            blob.delete()
            blob = bucket.create(blobName, value, binaryMimeType)
        }
    }

    override fun get(key: StoreKey): ByteArray? {
        val blobName = key.toString()
        var blob = bucket.get(blobName) ?: return null
        return blob.getContent()
    }

    override fun close() {

    }
}