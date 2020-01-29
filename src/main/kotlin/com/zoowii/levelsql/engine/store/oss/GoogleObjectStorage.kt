package com.zoowii.levelsql.engine.store.oss

import com.google.cloud.storage.*
import java.io.OutputStream
import java.lang.Exception

class GoogleBlob(val blob: Blob) : BlobProxy {
    override fun getContent(): ByteArray {
        return blob.getContent()
    }

    override fun reload(): BlobProxy {
        return GoogleBlob(blob.reload())
    }

    override fun delete(): Boolean {
        return blob.delete()
    }

    override fun downloadTo(out: OutputStream) {
        blob.downloadTo(out)
    }

}

class GoogleBucket(val bucket: Bucket) : BucketProxy {
    override fun get(blobName: String): BlobProxy? {
        try {
            val blob = bucket.get(blobName)
            if (blob == null) {
                return null
            }
            return GoogleBlob(blob)
        } catch(e: Exception) {
            e.printStackTrace()
            return null
        }
    }

    override fun create(blobName: String, content: ByteArray, contentType: String): BlobProxy {
        val blob = bucket.create(blobName, content, contentType)
        return GoogleBlob(blob)
    }
}

class GoogleObjectStorage : ObjectStorage {
    private var storage: Storage? = null

    fun initStorage() {
        storage = StorageOptions.getDefaultInstance().service
    }

    override fun createBucket(name: String): BucketProxy {
        val bucket = storage!!.create(BucketInfo.of(name))
        return GoogleBucket(bucket)
    }

    override fun getBucket(name: String): BucketProxy {
        val bucket = storage!!.get(name)
        return GoogleBucket(bucket)
    }
}