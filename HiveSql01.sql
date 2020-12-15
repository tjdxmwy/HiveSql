一、库的DDL

beeline -u jdbc:hive2://hadoop102:10000 -n atguigu	#开启hive

--建库

CREATE DATABASE [IF NOT EXISTS] database_name
[COMMENT database_comment]
[LOCATION hdfs_path]
[WITH DBPROPERTIES (property_name=property_value, ...)];

-- eg

CREATE DATABASE IF NOT EXISTS test
COMMENT "Just For Test"
location "/test.db"
WITH DBPROPERTIES ("aaa"="bbb");

--查看所有库
show databases;
--查看库信息
desc DATABASE test;
--查看详细信息
desc DATABASE extended test;
--修改库
alter DATABASE test set dbproperties("aaa"="ccc");
--删库
drop database test
--强制删库
drop database test cascade;

二、表的DDL

CREATE [EXTERNAL] TABLE [IF NOT EXISTS] table_name 
[(col_name data_type [COMMENT col_comment], ...)] 
[COMMENT table_comment] 
[PARTITIONED BY (col_name data_type [COMMENT col_comment], ...)] 
[CLUSTERED BY (col_name, col_name, ...) 
[SORTED BY (col_name [ASC|DESC], ...)] INTO num_buckets BUCKETS] 
[ROW FORMAT row_format] 
[STORED AS file_format] 
[LOCATION hdfs_path]
[TBLPROPERTIES (property_name=property_value, ...)]
[AS select_statement]

--创建表

CREATE TABLE IF NOT EXISTS test 
(id int comment "ID", name string comment "Name")
comment "Test Table"
row format delimited fields terminated by "\t"
location "/test_tb"
TBLPROPERTIES("aaa" = "bbb");

--查表
desc test;
desc formatted test;

-- 上传数据
load data local inpath "/opt/module/datas/location.txt" into table test;

--修改一列
alter table test2 change id id string;
--添加一列或多列
alter table test2 add columns(class string comment "class NO.");
--替换多列信息
alter table test2 replace columns(id double comment "ID" , name string);
--删表
drop table test2;

--用查询结果建表
create table stu_result as select * from stu_par where id=1001;


三、内外部表

1.  建立外部表

--建立外部表
create external table test
(id int, name string)
row format delimited fields terminated by '\t';

--插入数据
load data local inpath "/opt/module/datas/student.txt" into table test;

--删表后查看HDFS，数据还在
drop table test;

2.  外部表和内部表转换

--建立错成内部表
create table test
(id int, name string)
row format delimited fields terminated by '\t';

--转换成外部表
alter table test set tblproperties("EXTERNAL"="TRUE");

--转换成内部表
alter table test set tblproperties("EXTERNAL"="FALSE");

四、分区表
1. 一级分区表

--建立一张分区表
create table stu_par
(id int, name string)
partitioned by (class string)
row format delimited fields terminated by '\t';

--向表中插入数据
load data local inpath '/opt/module/datas/student.txt' into table stu_par
partition (class='01');

load data local inpath '/opt/module/datas/student.txt' into table stu_par
partition (class='02');

load data local inpath '/opt/module/datas/student.txt' into table stu_par
partition (class='03');

--查表时，选择分区，可以减小数据扫描量
select * from stu_par where class="01";
select * from stu_par where id=1001;

--查询分区表的分区
show partitions stu_par;

--如果提前准备数据，但是没有元数据，修复方式
--1. 添加分区
alter table stu_par add partition(class="03");
--2. 直接修复
msck repair table stu_par;
--3. 上传时候直接带分区
load data local inpath '/opt/module/datas/student.txt' into table stu_par
partition (class='03');


2. 二级分区表

--建立二级分区表
create table stu_par2
(id int, name string)
partitioned by (grade string, class string)
row format delimited fields terminated by '\t';

--插入数据，指定到二级分区
load data local inpath '/opt/module/datas/student.txt' into table stu_par2
partition (grade='01', class='03');

3. 分区的增删改查

--增加分区
alter table stu_par add partition(class="05");

--一次增加多个分区
alter table stu_par add partition(class="06") partition(class="07");
--删除分区
alter table stu_par drop partition(class="05");
--一次删除多个分区
alter table stu_par drop partition(class="06"), partition(class="07");


五、DML

1. 数据导入
--从本地磁盘或者DHFS导入数据
load data [local] inpath '/opt/module/datas/student.txt' [overwrite] into table student [partition (partcol1=val1,…)];

（1）load data:表示加载数据
（2）local:表示从本地加载数据到hive表；否则从HDFS加载数据到hive表
（3）inpath:表示加载数据的路径
（4）overwrite:表示覆盖表中已有数据，否则表示追加
（5）into table:表示加载到哪张表
（6）student:表示具体的表
（7）partition:表示上传到指定分区

--例子
load data local inpath '/opt/module/datas/student.txt' overwrite into table student;

--先在hdfs://hadoop102:8020/xxx文件夹上传一份student.txt
--HDFS的导入是移动，而本地导入是复制
load data inpath '/xxx/student.txt' overwrite into table student;

--Insert导入
insert into table student select id, name from stu_par where class="01";

--用查询结果建表
create table stu_result as select * from stu_par where id=1001;

2. 数据导出

--Insert导出
insert overwrite local directory '/opt/module/datas/export/student'
select * from student;

--带格式导出
insert overwrite local directory '/opt/module/datas/export/student1'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
select * from student;

#bash命令行导出
hive -e "select * from default.student;" > /opt/module/datas/export/test.txt

--整张表export到HDFS
export table student to '/export/student';

--从导出结果导入到Hive
import table student3 from '/export/student';

3. 数据删除
--只删表数据，不删表本身
truncate table student;