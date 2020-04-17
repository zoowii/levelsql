levelsql事务模型
=================

levelsql的事务采用undo log + Percolator两阶段提交算法 实现

没单纯使用Percolator算法的原因是事务目前没做到存储层，回滚时不方便直接删除旧数据(因为直接改的旧数据，而不是每次修改都创建一条新KV记录)

一个Table的Row中可选的有额外两列隐藏列L列和W列

L列用来加锁startTs+sessionId+optional-primaryRowId，W列记录commitTs
因为Row记录中只有值没有列信息，但是Table操作Row的时候能知道table中有多少列，
所以可以给row记录最后增加2列，如果只有L列则是L+W(null),如果只有W列则是L(null)+W
