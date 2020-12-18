一、常用函数

-- 查看所有的函数：
show functions;
desc function [extended] nvl; # 查看函数功能

1.1 空字段赋值

NVL：给值为NULL的数据赋值，它的格式是NVL( value，default_value)。
它的功能是如果value为NULL，则NVL函数返回default_value的值，否则返回value的值，如果两个参数都为NULL ，则返回NULL。

select comm, nvl(comm, -1) from emp;

+---------+---------+
|  comm   |   _c1   |
+---------+---------+
| NULL    | -1.0    |
| 300.0   | 300.0   |
| 500.0   | 500.0   |
| NULL    | -1.0    |
| 1400.0  | 1400.0  |
| NULL    | -1.0    |
| NULL    | -1.0    |
| NULL    | -1.0    |
| NULL    | -1.0    |
| 0.0     | 0.0     |
| NULL    | -1.0    |
| NULL    | -1.0    |
| NULL    | -1.0    |
| NULL    | -1.0    |
+---------+---------+

1.2 case when

需求：
	将如下数据转换成不同部门男女各多少人

-- name		dept_id		sex
-- 悟空		A			男
-- 大海		A			男
-- 宋宋		B			男
-- 凤姐		A			女
-- 婷姐		B			女
-- 婷婷		B			女


-- A     2       1
-- B     1       2

-- 建表
create table if not exists emp_sex
(name string, dept_id string, sex string)
row format delimited fields terminated by '\t';

load data local inpath "/opt/module/datas/emp_sex.txt" into table emp_sex;

-- 实现
select dept_id,
sum(case sex when "男" then 1 else 0 end) as male,
sum(case sex when "女" then 1 else 0 end) as female
from emp_sex
group by dept_id;


1.3 行转列

create table person_info(
name string, 
constellation string, 
blood_type string) 
row format delimited fields terminated by "\t";
load data local inpath "/opt/module/datas/constellation.txt" into table person_info;

-- 实现

select concat(constellation, ",", blood_type) xy,
concat_ws("|", collect_list(name)) as name
from person_info
group by constellation, blood_type;

+--------+-----------------+
|   xy   |   name          |
+--------+-----------------+
| 射手座,A  | 大海|凤姐      |
| 白羊座,A  | 孙悟空|猪八戒  |
| 白羊座,B  | 宋宋|苍老师    |
+--------+-----------------+

1.4 列转行

EXPLODE(col)：将hive一列中复杂的array或者map结构拆分成多行。
LATERAL VIEW
	用法：LATERAL VIEW udtf(expression) tableAlias AS columnAlias
	解释：用于和split, explode等UDTF一起使用，它能够将一列数据拆成多行数据，在此基础上可以对拆分后的数据进行聚合。

create table movie_info(
movie string, 
category string) 
row format delimited fields terminated by "\t"
collection items terminated by ",";
load data local inpath "/opt/module/datas/movie.txt" into table movie_info;

-- 实现

select m.movie, tbl.cate
from movie_info m
LATERAL VIEW
explode(split(category, ",")) tbl as cate;

-- 《疑犯追踪》      	悬疑
-- 《疑犯追踪》      	动作
-- 《疑犯追踪》      	科幻
-- 《疑犯追踪》      	剧情
-- 《Lie to me》   	悬疑
-- 《Lie to me》   	警匪
-- 《Lie to me》   	动作
-- 《Lie to me》   	心理
-- 《Lie to me》   	剧情
-- 《战狼2》        	战争
-- 《战狼2》        	动作
-- 《战狼2》        	灾难

select b.movie, concat_ws("|",collect_list(b.cate))
from 
  (select m.movie as movie, tbl.cate as cate
  from movie_info m 
  LATERAL VIEW 
  explode(split(category, ",")) tbl as cate) b
group by b.movie;

+--------------+-----------------+
|   b.movie    |       _c1       |
+--------------+-----------------+
| 《Lie to me》  | 悬疑|警匪|动作|心理|剧情  |
| 《战狼2》       | 战争|动作|灾难        |
| 《疑犯追踪》    | 悬疑|动作|科幻|剧情     |
+--------------+-----------------+


二、窗口函数

2.1 创建hive表并导入数据

create table if not exists business(
name string,
orderdate string,
cost int)
row format delimited fields terminated by ",";
load data local inpath "/opt/module/datas/business.txt" into table business;

(1)查询在2017年4月份购买过的顾客及总人数
select name,count(*) over () 
from business 
where substring(orderdate,1,7) = '2017-04' 
group by name;

+-------+-----------------+
| name  | count_window_0  |
+-------+-----------------+
| mart  | 2               |
| jack  | 2               |
+-------+-----------------+

(2)查询顾客的购买明细及月购买总额

select name, cost, orderdate,substring(orderdate, 6, 2),
sum(cost) over(partition by substring(orderdate, 6, 2))
from business;

-- jack	10	2017-01-01	01	205
-- jack	55	2017-01-08	01	205
-- tony	50	2017-01-07	01	205
-- jack	46	2017-01-05	01	205
-- tony	29	2017-01-04	01	205
-- tony	15	2017-01-02	01	205
-- jack	23	2017-02-03	02	23
-- mart	94	2017-04-13	04	341
-- jack	42	2017-04-06	04	341
-- mart	75	2017-04-11	04	341
-- mart	68	2017-04-09	04	341
-- mart	62	2017-04-08	04	341
-- neil	12	2017-05-10	05	12
-- neil	80	2017-06-12	06	80

(3)上述的场景, 将每个顾客的cost按照日期进行累加
select name, cost, orderdate,
sum(cost) over() as sample1,  -- 所有行相加
sum(cost) over(partition by name) as sample2,  --按name分组，组内数据相加
sum(cost) over(partition by name order by orderdate) as sample3,  --按name分组，组内数据累加
sum(cost) over(partition by name order by orderdate rows between unbounded preceding and current row) as sample4,  --和sample3一样,由起点到当前行的聚合
sum(cost) over(partition by name order by orderdate rows between 1 preceding and current row) as sample5,  --当前行和前面一行做聚合
sum(cost) over(partition by name order by orderdate rows between 1 preceding and 1 following) as sample6,--当前行和前边一行及后面一行
sum(cost) over(partition by name order by orderdate rows between current row and unbounded following) as sample7--当前行及后面所有行
from business;


+-------+-------+-------------+----------+----------+----------+----------+----------+----------+----------+
| name  | cost  |  orderdate  | sample1  | sample2  | sample3  | sample4  | sample5  | sample6  | sample7  |
+-------+-------+-------------+----------+----------+----------+----------+----------+----------+----------+
| jack  | 10    | 2017-01-01  | 661      | 176      | 10       | 10       | 10       | 56       | 176      |
| jack  | 46    | 2017-01-05  | 661      | 176      | 56       | 56       | 56       | 111      | 166      |
| jack  | 55    | 2017-01-08  | 661      | 176      | 111      | 111      | 101      | 124      | 120      |
| jack  | 23    | 2017-02-03  | 661      | 176      | 134      | 134      | 78       | 120      | 65       |
| jack  | 42    | 2017-04-06  | 661      | 176      | 176      | 176      | 65       | 65       | 42       |
| mart  | 62    | 2017-04-08  | 661      | 299      | 62       | 62       | 62       | 130      | 299      |
| mart  | 68    | 2017-04-09  | 661      | 299      | 130      | 130      | 130      | 205      | 237      |
| mart  | 75    | 2017-04-11  | 661      | 299      | 205      | 205      | 143      | 237      | 169      |
| mart  | 94    | 2017-04-13  | 661      | 299      | 299      | 299      | 169      | 169      | 94       |
| neil  | 12    | 2017-05-10  | 661      | 92       | 12       | 12       | 12       | 92       | 92       |
| neil  | 80    | 2017-06-12  | 661      | 92       | 92       | 92       | 92       | 92       | 80       |
| tony  | 15    | 2017-01-02  | 661      | 94       | 15       | 15       | 15       | 44       | 94       |
| tony  | 29    | 2017-01-04  | 661      | 94       | 44       | 44       | 44       | 94       | 79       |
| tony  | 50    | 2017-01-07  | 661      | 94       | 94       | 94       | 79       | 79       | 50       |
+-------+-------+-------------+----------+----------+----------+----------+----------+----------+----------+


-- 总结：
-- 		1)rows必须跟在Order by子句之后，对排序的结果进行限制，使用固定的行数来限制分区中的数据行数量
-- 		2)current row是固定搭配, 注意preceding和following后面不接row
-- 		3)前面所有和后面所有行用unbounded, eg:unbounded preceding, unbounded following

(4) 查看顾客上次的购买时间
-- lag, lead
select name, cost, orderdate,
lag(orderdate, 1, "1970-01-01") over(partition by name order by orderdate) as time1,
lag(orderdate, 2, "1970-01-01") over(partition by name order by orderdate) as time2,
lead(orderdate, 1, "1970-01-01") over(partition by name order by orderdate) as time3,
lead(orderdate, 2, "1970-01-01") over(partition by name order by orderdate) as time4
from business;

+-------+-------+-------------+-------------+-------------+-------------+-------------+
| name  | cost  |  orderdate  |    time1    |    time2    |    time3    |    time4    |
+-------+-------+-------------+-------------+-------------+-------------+-------------+
| jack  | 10    | 2017-01-01  | 1970-01-01  | 1970-01-01  | 2017-01-05  | 2017-01-08  |
| jack  | 46    | 2017-01-05  | 2017-01-01  | 1970-01-01  | 2017-01-08  | 2017-02-03  |
| jack  | 55    | 2017-01-08  | 2017-01-05  | 2017-01-01  | 2017-02-03  | 2017-04-06  |
| jack  | 23    | 2017-02-03  | 2017-01-08  | 2017-01-05  | 2017-04-06  | 1970-01-01  |
| jack  | 42    | 2017-04-06  | 2017-02-03  | 2017-01-08  | 1970-01-01  | 1970-01-01  |
| mart  | 62    | 2017-04-08  | 1970-01-01  | 1970-01-01  | 2017-04-09  | 2017-04-11  |
| mart  | 68    | 2017-04-09  | 2017-04-08  | 1970-01-01  | 2017-04-11  | 2017-04-13  |
| mart  | 75    | 2017-04-11  | 2017-04-09  | 2017-04-08  | 2017-04-13  | 1970-01-01  |
| mart  | 94    | 2017-04-13  | 2017-04-11  | 2017-04-09  | 1970-01-01  | 1970-01-01  |
| neil  | 12    | 2017-05-10  | 1970-01-01  | 1970-01-01  | 2017-06-12  | 1970-01-01  |
| neil  | 80    | 2017-06-12  | 2017-05-10  | 1970-01-01  | 1970-01-01  | 1970-01-01  |
| tony  | 15    | 2017-01-02  | 1970-01-01  | 1970-01-01  | 2017-01-04  | 2017-01-07  |
| tony  | 29    | 2017-01-04  | 2017-01-02  | 1970-01-01  | 2017-01-07  | 1970-01-01  |
| tony  | 50    | 2017-01-07  | 2017-01-04  | 2017-01-02  | 1970-01-01  | 1970-01-01  |
+-------+-------+-------------+-------------+-------------+-------------+-------------+

(5) 查询前20%时间的订单信息
-- ntile
select *
from 
  (select name, cost, orderdate,
  ntile(5) over(order by orderdate) sorted
  from business) t
where t.sorted=1;

+---------+---------+--------------+-----------+
| t.name  | t.cost  | t.orderdate  | t.sorted  |
+---------+---------+--------------+-----------+
| jack    | 10      | 2017-01-01   | 1         |
| tony    | 15      | 2017-01-02   | 1         |
| tony    | 29      | 2017-01-04   | 1         |
+---------+---------+--------------+-----------+

--percent_rank
select
	name,
	orderdate,
	cost,
	PERCENT_RANK() over(
	order by orderdate) pr
from
	business;

2.2 rank, dense_rank, row_number

-- RANK() 排序相同时会重复，总数不会变
-- DENSE_RANK() 排序相同时会重复，总数会减少
-- ROW_NUMBER() 会根据顺序计算

创建hive表并导入数据
create table score(
name string,
subject string, 
score int) 
row format delimited fields terminated by "\t";
load data local inpath '/opt/module/datas/score.txt' into table score;

-- rank
select *,
rank() over(partition by subject order by score desc) as rk,
dense_rank() over(partition by subject order by score desc) as rd,
row_number() over(partition by subject order by score desc) as rn
from score;


+-------------+----------------+--------------+-----+-----+-----+
| score.name  | score.subject  | score.score  | rk  | rd  | rn  |
+-------------+----------------+--------------+-----+-----+-----+
| 孙悟空       | 数学             | 95           | 1   | 1   | 1   |
| 宋宋         | 数学             | 86           | 2   | 2   | 2   |
| 婷婷         | 数学             | 85           | 3   | 3   | 3   |
| 大海         | 数学             | 56           | 4   | 4   | 4   |
| 宋宋         | 英语             | 84           | 1   | 1   | 1   |
| 大海         | 英语             | 84           | 1   | 1   | 2   |
| 婷婷         | 英语             | 78           | 3   | 2   | 3   |
| 孙悟空       | 英语             | 68           | 4   | 3   | 4   |
| 大海         | 语文             | 94           | 1   | 1   | 1   |
| 孙悟空       | 语文             | 87           | 2   | 2   | 2   |
| 婷婷         | 语文             | 65           | 3   | 3   | 3   |
| 宋宋         | 语文             | 64           | 4   | 4   | 4   |
+-------------+----------------+--------------+-----+-----+-----+


三、时间函数
show functions like "*date*";

+---------------+
|   tab_name    |
+---------------+
| current_date  |
| date_add      |
| date_format   |
| date_sub      |
| datediff      |
| to_date       |
+---------------+

--current_date 返回当前日期
select current_date();

--日期的加减
--今天开始90天以后的日期
select date_add(current_date(), 90);
--今天开始90天以前的日期
select date_sub(current_date(), 90);

--日期差
select date_diff(current_date(), "1993-05-14");

-- result:10079

-- date_format
select date_format(current_date(), "y");
select date_format(current_date(), "M");
select date_format(current_date(), "d");

-- to_date
select to_date("2020-12-17 10:55:00");

-- result: 2020-12-17


四、练习

习题：有哪些顾客连续两天来过我的店，数据是business

-- 1.对name分区并且按照时间排序 
select *,
row_number() over(partition by name order by orderdate) as rn 
from business;
-- 2.将日期减去排序rn,如果得到的时间date2相等，证明是连续两天来的顾客
select t.name, t.orderdate, t.cost, t.rn, date_sub(t.orderdate, t.rn) as date2
from 
(select name, orderdate, cost,
row_number() over(partition by name order by orderdate) as rn 
from business) t;

+---------+--------------+---------+-------+-------------+
| t.name  | t.orderdate  | t.cost  | t.rn  |    date2    |
+---------+--------------+---------+-------+-------------+
| jack    | 2017-01-01   | 10      | 1     | 2016-12-31  |
| jack    | 2017-01-05   | 46      | 2     | 2017-01-03  |
| jack    | 2017-01-08   | 55      | 3     | 2017-01-05  |
| jack    | 2017-02-03   | 23      | 4     | 2017-01-30  |
| jack    | 2017-04-06   | 42      | 5     | 2017-04-01  |
| mart    | 2017-04-08   | 62      | 1     | 2017-04-07  |
| mart    | 2017-04-09   | 68      | 2     | 2017-04-07  |
| mart    | 2017-04-11   | 75      | 3     | 2017-04-08  |
| mart    | 2017-04-13   | 94      | 4     | 2017-04-09  |
| neil    | 2017-05-10   | 12      | 1     | 2017-05-09  |
| neil    | 2017-06-12   | 80      | 2     | 2017-06-10  |
| tony    | 2017-01-02   | 15      | 1     | 2017-01-01  |
| tony    | 2017-01-04   | 29      | 2     | 2017-01-02  |
| tony    | 2017-01-07   | 50      | 3     | 2017-01-04  |
+---------+--------------+---------+-------+-------------+

-- 3.对name和date2分组，找出大于2次的人员

select t2.name, count(*) c
from 
(select t.name, t.orderdate, t.cost, t.rn, date_sub(t.orderdate, t.rn) as date2
from 
(select name, orderdate, cost,
row_number() over(partition by name order by orderdate) as rn 
from business) t) t2
group by t2.name, t2.date2 
having count(*) >= 2;

+----------+----+
| t2.name  | c  |
+----------+----+
| mart     | 2  |
+----------+----+