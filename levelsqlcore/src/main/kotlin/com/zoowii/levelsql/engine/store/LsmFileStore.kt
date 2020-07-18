package com.zoowii.levelsql.engine.store

/**
 * TODO: 使用日志结构合并树(LSM)格式的本地磁盘文件的store实现
 * 目前用单个文件+单层LSM树的存储方式简化实现。这里不要求key有序
 * 文件格式:
 * file-header:
 * blocks: block-header + block-body
 * block-header包含各记录数量，各记录长度和开始位置偏移,块中各记录的merkle-tree.
 * block-body包含各记录具体数据以及记录是否删除，记录的修改时间戳
 *
 */
class LsmFileStore {
}