一、自定义函数

1.1 编程步骤：
	(1)继承org.apache.hadoop.hive.ql.exec.UDF
	(2)需要实现evaluate函数；evaluate函数支持重载；
	(3)在hive的命令行窗口创建函数
	添加jar
		add jar linux_jar_path
	创建function
		create [temporary] function [dbname.]function_name AS class_name;
	(4)在hive的命令行窗口删除函数
		Drop [temporary] function [if exists] [dbname.]function_name;

1.2 注意事项：UDF必须要有返回类型，可以返回null，但是返回类型不能为void.

1.3 具体步骤
	-- 1. 打成jar包上传到服务器/opt/module/datas
	-- 2. 将jar包添加到hive的classpath
	add jar /opt/module/datas/hive-udf-1.0-SNAPSHOT.jar;
	-- 3. 创建临时函数与开发好的java class关联
	create temporary function myudf as "com.atguigu.udf.Lower";
	-- 4.测试
	select ename, myudf(ename) from emp limit 5;

		+---------+---------+	
		|  ename  |   _c1   |
		+---------+---------+
		| SMITH   | smith   |
		| ALLEN   | allen   |
		| WARD    | ward    |
		| JONES   | jones   |
		| MARTIN  | martin  |
		+---------+---------+


二、存储和压缩对比
1.开启Map输出阶段压缩（MR引擎）
	-- 开启hive中间传输数据压缩功能
	set hive.exec.compress.intermediate=true;
	-- 开启mapreduce中map输出压缩功能
	set mapreduce.map.output.compress=true;
	-- 设置mapreduce中map输出数据的压缩方式
	set mapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;
	-- 执行查询语句
	select count(ename) name from emp;

2.开启Reduce输出阶段压缩
	-- 开启hive最终输出数据压缩功能
		set hive.exec.compress.output=true;
	-- 开启mapreduce最终输出数据压缩
		set mapreduce.output.fileoutputformat.compress=true;
	-- 设置mapreduce最终数据输出压缩方式
		set mapreduce.output.fileoutputformat.compress.codec = org.apache.hadoop.io.compress.SnappyCodec;
	-- 设置mapreduce最终数据输出压缩为块压缩
		set mapreduce.output.fileoutputformat.compress.type=BLOCK;
	-- 测试一下输出结果是否是压缩文件
		insert overwrite local directory '/opt/module/datas/distribute-result' 
		select * from emp distribute by deptno sort by empno desc;


3. 比较各种存储格式
--建立文本表格
create table log_text (
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string)
row format delimited fields terminated by '\t'
stored as textfile;

load data local inpath '/opt/module/datas/log.data' into table log_text;

--建立非压缩的orc格式
create table log_orc (
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string)
row format delimited fields terminated by '\t'
stored as orc 
tblproperties("orc.compress"="NONE");

insert into table log_orc select * from log_text;

--建立parquet格式
create table log_par (
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string)
row format delimited fields terminated by '\t'
stored as parquet;

insert into log_par select * from log_text;

-- textfile: 18.13 MB
-- orc: 7.69 MB
-- parquet: 13.09 MB

4. 比较各种压缩格式

--zlib压缩的orc格式
create table log_orc_zlib (
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string)
row format delimited fields terminated by '\t'
stored as orc 
tblproperties("orc.compress"="ZLIB");

insert into log_orc_zlib select * from log_text;

--snappy压缩的orc格式
create table log_orc_snappy (
track_time string,
url string,
session_id string,
referer string,
ip string,
end_user_id string,
city_id string)
row format delimited fields terminated by '\t'
stored as orc 
tblproperties("orc.compress"="snappy");

insert into log_orc_snappy select * from log_text;


三、表的优化

1.小表、大表Join
-- 创建大表
create table bigtable(
id bigint,
time2 bigint, 
uid string, 
keyword string, 
url_rank int, 
click_num int, 
click_url string)
row format delimited fields terminated by '\t';

-- 创建小表
create table smalltable(
id bigint, 
time2 bigint, 
uid string, 
keyword string, 
url_rank int, 
click_num int, 
click_url string)
row format delimited fields terminated by '\t';

-- 创建join后表的语句
create table jointable (
id bigint,
time2 bigint,
uid string,
keyword string, 
url_rank int, 
click_num int, 
click_url string) 
row format delimited fields terminated by '\t';

-- 分别向大表和小表中导入数据
load data local inpath "/opt/module/datas/bigtable" into table bigtable;
load data local inpath "/opt/module/datas/smalltable" into table smalltable;

-- 关闭mapjoin功能（默认是打开的）
set hive.auto.convert.join = false;

-- 执行小表JOIN大表语句
insert overwrite table jointable
select b.id, b.time2, b.uid, b.keyword, b.url_rank, b.click_num, b.click_url
from smalltable a 
join bigtable b 
on a.id = b.id; 

-- 执行大表JOIN小表语句
insert overwrite table jointable
select b.id, b.time2, b.uid, b.keyword, b.url_rank, b.click_num, b.click_url
from bigtable b 
join smalltable a 
on b.id = a.id; 

2.大表Join大表

--建没有Null的表
create table ori(
id bigint,
t bigint,
uid string, 
keyword string, 
url_rank int, 
click_num int, 
click_url string) 
row format delimited fields terminated by '\t';

load data local inpath '/opt/module/datas/ori' into table ori;

--建立有Null的表
create table nullidtable(
id bigint, 
t bigint, 
uid string, 
keyword string, 
url_rank int, 
click_num int, 
click_url string) 
row format delimited fields terminated by '\t';

load data local inpath '/opt/module/datas/nullid' into table nullidtable;

-- 设置5个reduce个数
set mapreduce.job.reduces = 5;

--空key过滤
insert overwrite table jointable
select n.*
from (select * from nullidtable where uid is not null) n
left join ori o 
on n.id = o.id;

--空key转换
insert overwrite table jointable
select n.* from nullidtable n 
full join ori o on 
nvl(n.id,rand()) = o.id;

3.动态分区调整

-- 3.1 开启动态分区参数设置
-- 开启动态分区功能（默认true，开启）
hive.exec.dynamic.partition=true

-- 设置为非严格模式（动态分区的模式，默认strict，表示必须指定至少一个分区为静态分区，
-- nonstrict模式表示允许所有的分区字段都可以使用动态分区。）
hive.exec.dynamic.partition.mode=nonstrict

-- 在所有执行MR的节点上，最大一共可以创建多少个动态分区。
hive.exec.max.dynamic.partitions=1000

-- 在每个执行MR的节点上，最大可以创建多少个动态分区。该参数需要根据实际的数据来设定。
-- 比如：源数据中包含了一年的数据，即day字段有365个值，那么该参数就需要设置成大于365，如果使用默认值100，则会报错。
hive.exec.max.dynamic.partitions.pernode=100

-- 整个MR Job中，最大可以创建多少个HDFS文件。
hive.exec.max.created.files=100000

-- 当有空分区生成时，是否抛出异常。一般不需要设置。
hive.error.on.empty.partition=false


--首先设置非严格模式
set hive.exec.dynamic.partition.mode=nonstrict;

--创建分区表
create table dept_partition(id int, name string)
partitioned by (location int) 
row format delimited fields terminated by '\t';

--从原表中向分区表插入数据
insert into table dept_partition partition(location)
select deptno, dname, loc from dept;

4. 执行计划（Explain）

-- 基本语法
EXPLAIN [EXTENDED | DEPENDENCY | AUTHORIZATION] query

explain select * from emp;
explain EXTENDED select * from emp;