LevelSQL
========

simple relational sql database stored on everything(disk file, leveldb, redis, oss, etc.)


# Notice
* just for fun and learning

# features
* mysql protocol(兼容MySQL网络协议，支持使用MySQL客户端驱动连接使用)
* B+ tree based index (基于B+树的索引)
* SQL Syntax(带有高效的SQL接口，可以通过SQL执行CRUD，以及支持索引查找，join操作，排序，分组，聚合函数等语法)
* transaction(支持事务)
* using index when available(SQL执行时如果发现可以用索引优化检索的时候优化执行计划)
* logic planner and optimizer(SQL执行计划和执行计划的优化器)
* table cluster index and multi-columns secondary index(支持聚集索引和二级索引，联合索引)
* local file store (直接磁盘文件做存储层)
* KV db store (在KV数据库比如leveldb/redis之上作为存储层)
* OSS store(把对象存储作为存储层)

# TODO
* performance test
* physical optimizer
* skiplist based index in memory
* LSM based store and index

# Core Components
* MySQL protocol(基于TCP的MySQL网络协议，可以通过MySQL客户端驱动使用LevelSQL)
* SQL parser(SQL语法解析器)
* disk-based B-plus tree(基于慢存储的B+树的实现)
* ordered KV store APIs(有序KV的IStore)
* IStore implemention based on disk-file or leveldb or OSS or redis (IStore可以有多种底层实现)
* logical planners and planner builder(将SQL抽象语法树转换成一个执行计划树)
* planner optimizer(执行计划的优化器，比如将表检索planner转换成索引查找planner等优化)
* planner executor(执行计划的执行器)
* transaction(事务)

# Example

* You can use LevelSQL as a mysql server or as a library(embedded sql engine)

* If you use LevelSQL as mysql server, you can use any existed mysql client/driver to connect to it

``` 
eg.
# start levelsql server
java -jar levelsql.jar -d data_dir -h 127.0.0.1 -p 3000

# use levelsql by Python and pymysql(or use can use Java+JDBC or any other mysql client/driver)
import pymysql

db = pymysql.connect(host='127.0.0.1', port=3000, db='test', user='test', passwd='pass')

cur = db.cursor()
cur.execute('show databases')
data = cur.fetchall()
print('databases', data)

cur.execute('use test')
cur.execute('select * from employee')
data = cur.fetchall()
print('employees', data)
```

* create database/table/index(as a library)
``` 
run {
    val session = engine.createSession()
    engine.executeSQL(session, "create database test")
    session.useDb("test")
    engine.executeSQL(session, "create table person (id int, person_name text)")
    engine.executeSQL(session, "create index person_name_idx on person (per_name)")
}
```

* select
```
select name, age, * from employee, person 
    left join country on employee.country_id=country.id 
    where age >= 18 
    order by id desc group by age limit 10,20
```

```
    val engine = LevelSqlEngine(store!!)
    engine.loadMeta()
    val session = engine.createSession()
    session.useDb("test")
    val sql1 = "select name, age, * from employee where id > 1 order by id desc limit 1,2"
    engine.executeSQL(session, sql1)

    // debug log output:
    [Test worker] DEBUG com.zoowii.levelsql.engine.LevelSqlEngine - logical planner before optimise:
    projection name, age, *
    limit 1, 2
    order by id desc
    filter by id > 1
    select employee
    [Test worker] DEBUG com.zoowii.levelsql.engine.LevelSqlEngine - logical planner optimised:
    projection name, age, *
    limit 1, 2
    order by id desc
    filter by id > 1
    index select employee by index employee_primary_id asc id > 1 
    [Test worker] DEBUG com.zoowii.levelsql.engine.planner.IndexSelectPlanner - index select planner fetched row: 2,	zhang2,	22
    [Test worker] DEBUG com.zoowii.levelsql.engine.planner.IndexSelectPlanner - index select planner fetched row: 3,	zhang3,	23
    [Test worker] DEBUG com.zoowii.levelsql.engine.planner.IndexSelectPlanner - table employee select end
    [Test worker] DEBUG com.zoowii.levelsql.engine.LevelSqlEngine - result:
    name	age	id	name	age	country_id
    zhang3,	23,	3,	zhang3,	23,	null
    zhang2,	22,	2,	zhang2,	22,	null
```

``` 
select sum(age), count(age), max(age), min(age)
     from employee, person
     where id > 0 limit 2,2

// output:
sum(age)	count(age)	max(age)	min(age)
44,	2,	23,	21
```

* insert
``` 
insert into employee (id, name, age) values 
    (1, 'zhang1', 21), (2, 'zhang2', 22+2), (3, 'zhang3', 23)
```

* update
```
update employee set name = 'wang8', age = 30 where id=1
```

* delete
```
delete from employee where age >= 18
```

* alter
```
alter table employee add gender text, add age int
```
