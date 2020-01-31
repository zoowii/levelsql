levelsql
========

simple relational sql database

# Notice
* just for fun and learning

# features
* B+ tree based index (基于B+树的索引)
* add/update/delete/seek-by-condition(增删改查和按照条件进行索引查找数据)
* SQL parser
* local file store (直接磁盘文件做存储层)
* KV db store (在KV数据库比如leveldb/redis之上作为存储层)
* OSS store(把对象存储作为存储层)

# DOING
* logic planner/physical planner/optimizer

# TODO
* performance test
* mysql protocol
* skiplist based index in memory
* LSM based store and index
