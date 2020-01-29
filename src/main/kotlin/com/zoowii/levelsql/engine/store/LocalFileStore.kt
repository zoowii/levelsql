package com.zoowii.levelsql.engine.store

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

/**
 * 本地磁盘文件的store实现
 * 因为StoreKey是有格式的并且有序的，所以也可以用StoreKey中的nodeId(增加的整数作为块id来存取)，前缀作为文件夹路径。从而采用多文件存储
 */
class LocalFileStore(private val dirFile: File) : IStore {
    private val blockSize = 1024*1024; // 1 MB
    private val blockHeaderSize = 128; // 128 bytes

    companion object {
        @Throws(IOException::class)
        fun openStore(dirFile: File): IStore {
            if(!dirFile.exists()) {
                if (!dirFile.mkdirs()) {
                    throw IOException("can't create dir $dirFile")
                }
            }
            return LocalFileStore(dirFile)
        }
    }

    class FileBufferInfo(var buffer: MappedByteBuffer, var bufSize: Int)

    private fun openStoreFile(key: StoreKey): FileBufferInfo {
        val filePath = Paths.get(dirFile.absolutePath, key.namespace)
        val file = RandomAccessFile(filePath.toFile().absolutePath, "rw")
        if(!filePath.toFile().exists()) {
            if(!filePath.toFile().createNewFile()) {
                throw IOException("can't create file ${filePath.toAbsolutePath()}")
            }
        }
        val fc = file.channel
        val fileSize = file.length()
        val needSize = (key.getNonNegativeSeq() + 1) * blockSize
        val bufSize = if(fileSize>needSize) fileSize else needSize
        val buffer = fc.map(FileChannel.MapMode.READ_WRITE,
                0, bufSize)
        return FileBufferInfo(buffer, bufSize.toInt())
    }

    private val fileBuffers = ConcurrentHashMap<String, FileBufferInfo>()

    // 当需要的文件missing的时候，mmap打开数据文件并映射到内存。
    private fun getStoreFile(key: StoreKey): FileBufferInfo {
        return fileBuffers.getOrPut(key.toString(), {
            return openStoreFile(key)
        })
    }

    private val lengthBytesSize = 4 // block中记录ByteArray长度的字节数

    // 目前每个node都写入一个单独的block种，这样存储效率不高，需要一个block可以存储多个nodes的数据

    override fun put(key: StoreKey, value: ByteArray) {
        if(value.size > blockSize - lengthBytesSize) {
            throw IOException("block size exceed") // 目前只支持写入单节点不超过block大小的数据
        }
        val bufInfo = getStoreFile(key)
        val fileBuf = bufInfo.buffer
        val pos = key.getNonNegativeSeq() * blockSize
        val needSize = pos + blockSize

        if(needSize > bufInfo.bufSize) {
            // 如果fileBuf的大小不够用了，就要重新映射了
            fileBuf.limit(needSize.toInt())
            bufInfo.bufSize = needSize.toInt()
        }
        fileBuf.position(pos.toInt())
        fileBuf.put(value.toBytes())
    }

    override fun get(key: StoreKey): ByteArray? {
        val bufInfo = getStoreFile(key)
        val fileBuf = bufInfo.buffer
        val pos = key.getNonNegativeSeq() * blockSize
        if(pos+blockSize > bufInfo.bufSize) {
            return null
        }
        fileBuf.position(pos.toInt())
        val lenBytes = ByteArray(4)
        fileBuf.get(lenBytes)
        val len = Int32FromBytes(lenBytes).first
        if(len < 1) {
            return null
        }
        val valueBytes = ByteArray(len)
        fileBuf.get(valueBytes)
        return valueBytes
    }

    override fun close() {
        // flush未写入的数据到文件
        for(bufInfo in fileBuffers.values) {
            bufInfo.buffer.force()
        }
    }
}