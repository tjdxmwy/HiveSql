一、基本查询

--建库
create database if not exists atguigu
COMMENT "Just For Test"
WITH DBPROPERTIES ("aaa"="bbb");


--建立员工表
create table if not exists emp(
empno int,
ename string,
job string,
mgr int,
hiredate string, 
sal double, 
comm double,
deptno int)
row format delimited fields terminated by '\t';

--建立部门表
create table if not exists dept(
deptno int,
dname string,
loc int
)
row format delimited fields terminated by '\t';

--导入数据
load data local inpath '/opt/module/datas/dept.txt' into table dept;
load data local inpath '/opt/module/datas/emp.txt' into table emp;

--全表查询
select * from emp;
select * from dept;

--查询某些列
select empno, ename from emp;

--起别名
select ename as name from emp;
--as可以省略
select ename name from emp;

--运算符
select ename, sal + 10 from emp;

--UDF函数
select substring(ename, 1, 1) from emp;
--UDAF函数
select count(*) from emp;
--UDTF函数

--limit，取前几行
select * from emp limit 5;

二、条件过滤

--查询工资大于1000的人
select * from emp where sal > 1000;
-- 查询出薪水等于5000的所有员工
select * from emp where sal =5000;
-- 查询工资在500到1000的员工信息
select * from emp where sal between 500 and 1000;
-- 查询comm为空的所有员工信息
select * from emp where comm is null;
-- 查询工资是1500或5000的员工信息
select * from emp where sal IN (1500, 5000);

--通配符字符串匹配 % _
--以A开头的员工
select * from emp where ename like "A%";

--正则匹配
--以A开头的员工
select * from emp where ename rlike "^A";

【正则入门】
	一般字符匹配自己
	^ 匹配一行开头 ^R 以R开头
	$ 匹配一行结束 R$ 以R结尾
	. 匹配任意字符 ^.$ 一行只有一个字符
	* 前一个子式匹配零次或多次
	[] 匹配一个范围内的任意字符
	\ 转义

--与或非
select * from emp where empno = 30 and sal > 1000;


三、分组

--计算emp表每个部门的平均工资
select deptno, avg(sal) aa from emp group by deptno;

--分组过滤
--计算部门平均工资大于2000的部门
select deptno, avg(sal) aa from emp group by deptno having aa>2000;


四、连接

--查询员工编号，姓名以及部门所在名称
select
    e.empno,
    e.ename,
    d.dname
from
    emp e
join
    dept d
on
    e.deptno=d.deptno;

--多表连接
-- 建表
create table if not exists location(
loc int,
loc_name string)
row format delimited fields terminated by "\t";
-- 导入数据
load data local inpath "/opt/module/datas/location.txt" into table location;


SELECT e.ename, d.dname, l.loc_name
FROM   emp e 
JOIN   dept d
ON     d.deptno = e.deptno 
JOIN   location l
ON     d.loc = l.loc;


五、排序

--按照工资降序排序(全局排序)
select *
from emp
order by sal desc;


--多条件排序，先按部门排序，再按工资排序
select *
from emp
order by
deptno asc,
sal desc;

--一般需求不会要求给所有的数据排序，而要求求前几
--求工资前10的人,Map会先求局部前10
select *
from emp
order by sal desc
limit 10;


--还有一种可能，我们只需要看大概的数据趋势，不需要全排序
--Hive的局部排序
set mapreduce.job.reduces=3;
select * from emp sort by empno desc;
#此时任务数为3，将分成3个区进行排序 

-- result
+------------+------------+------------+----------+---------------+----------+-----------+-------------+
| emp.empno  | emp.ename  |  emp.job   | emp.mgr  | emp.hiredate  | emp.sal  | emp.comm  | emp.deptno  |
+------------+------------+------------+----------+---------------+----------+-----------+-------------+
| 7844       | TURNER     | SALESMAN   | 7698     | 1981-9-8      | 1500.0   | 0.0       | 30          |
| 7839       | KING       | PRESIDENT  | NULL     | 1981-11-17    | 5000.0   | NULL      | 10          |
| 7788       | SCOTT      | ANALYST    | 7566     | 1987-4-19     | 3000.0   | NULL      | 20          |
| 7782       | CLARK      | MANAGER    | 7839     | 1981-6-9      | 2450.0   | NULL      | 10          |
| 7698       | BLAKE      | MANAGER    | 7839     | 1981-5-1      | 2850.0   | NULL      | 30          |
| 7654       | MARTIN     | SALESMAN   | 7698     | 1981-9-28     | 1250.0   | 1400.0    | 30          |
| 7934       | MILLER     | CLERK      | 7782     | 1982-1-23     | 1300.0   | NULL      | 10          |
| 7900       | JAMES      | CLERK      | 7698     | 1981-12-3     | 950.0    | NULL      | 30          |
| 7876       | ADAMS      | CLERK      | 7788     | 1987-5-23     | 1100.0   | NULL      | 20          |
| 7566       | JONES      | MANAGER    | 7839     | 1981-4-2      | 2975.0   | NULL      | 20          |
| 7521       | WARD       | SALESMAN   | 7698     | 1981-2-22     | 1250.0   | 500.0     | 30          |
| 7499       | ALLEN      | SALESMAN   | 7698     | 1981-2-20     | 1600.0   | 300.0     | 30          |
| 7902       | FORD       | ANALYST    | 7566     | 1981-12-3     | 3000.0   | NULL      | 20          |
| 7369       | SMITH      | CLERK      | 7902     | 1980-12-17    | 800.0    | NULL      | 20          |
+------------+------------+------------+----------+---------------+----------+-----------+-------------+


--指定局部排序的分区字段
Distribute By： 在有些情况下，我们需要控制某个特定行应该到哪个reducer，通常是为了进行后续的聚集操作。
distribute by 子句可以做这件事。distribute by类似MR中partition（自定义分区），进行分区，结合sort by使用。

select * from emp distribute by empno sort by sal desc;
select * from emp distribute by deptno sort by sal desc;


--如果分区和排序的字段一样，我们可以用cluster by代替
select * from emp distribute by empno sort by empno;
select * from emp cluster by empno;

六、分桶
分区针对的是数据的存储路径；分桶针对的是数据文件。

-- (1)创建分桶表
create table stu_buck(id int, name string)
clustered by(id)
into 4 buckets
row format delimited fields terminated by "\t";

-- (2)查看表结构
desc formatted stu_buck;
-- Num Buckets: 4  

-- (3)导入数据到分桶表中
load data local inpath "/opt/module/datas/student.txt" into table stu_buck;

-- 需要设置这两个属性
set hive.enforce.bucketing=true;
set mapreduce.job.reduces=-1;