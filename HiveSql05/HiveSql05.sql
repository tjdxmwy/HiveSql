【Hive实战之谷粒影音】

需求描述
统计硅谷影音视频网站的常规指标，各种TopN指标：
--统计视频观看数Top10
--统计视频类别热度Top10
--统计视频观看数Top20所属类别
--统计视频观看数Top50所关联视频的所属类别Rank
--统计每个类别中的视频热度Top10
--统计每个类别中视频流量Top10
--统计上传视频最多的用户Top10以及他们上传的视频
--统计每个类别视频观看数Top10

数据结构

-- 视频表
video id		视频唯一id（String）			11位字符串
uploader		视频上传者（String）			上传视频的用户名String
age				视频年龄（int）				视频在平台上的整数天
category		视频类别（Array<String>）		上传视频指定的视频分类
length			视频长度（Int）				整形数字标识的视频长度
views			观看次数（Int）				视频被浏览的次数
rate			视频评分（Double）			满分5分
Ratings			流量（Int）					视频的流量，整型数字
conments		评论数（Int）				一个视频的整数评论数
related ids		相关视频id（Array<String>）	相关视频的id，最多20个


一.ETL原始数据
	-- 针对原始数据需要做清洗：所属类别数据(第四列)需要去除空格，最后相关视频relateId需要用&连接
	-- 清洗数据代码见etltool
	
	yarn jar etltool.jar com.atguigu.etl.ETLDriver /gulivedio/vedio /gulivedio/vedio_etl

二.数据准备

-- 在Hive中创建外部表映射数据
-- vedio_ori外部表
create external table vedio_ori (
vedio_id string,
uploader string,
age int,
category array<string>, 
length int,
views int,
rate float,
rating int,
comments string,
relateId array<string>)
row format delimited fields terminated by "\t"
collection items terminated by "&"
location "/gulivedio/vedio_etl"; 


--user_ori外部表
create external table user_ori (
uploader string,
vedios int,
friends int)
row format delimited fields terminated by "\t"
location "/gulivedio/user";

--video_orc内部表
create table if not exists vedio_orc (
vedio_id string,
uploader string,
age int,
category array<string>, 
length int,
views int,
rate float,
rating int,
comments string,
relateId array<string>)
row format delimited fields terminated by "\t"
collection items terminated by "&"
stored as orc
tblproperties("orc.compress"="SNAPPY");

--user_orc内部表
create table if not exists user_orc (
uploader string,
vedios int,
friends int)
row format delimited fields terminated by "\t"
collection items terminated by "&"
stored as orc 
tblproperties("orc.compress"="SNAPPY");

--从外部表中插入数据
insert into table vedio_orc select * from vedio_ori;
insert into table user_orc select * from user_ori;

三、实现需求

1. 统计视频观看数top10

select vedio_id,
views
from 
vedio_orc 
order by views desc
limit 10;

	+--------------+-----------+
	|   vedio_id   |   views   |
	+--------------+-----------+
	| dMH0bHeiRNg  | 42513417  |
	| 0XxI-hvPRRA  | 20282464  |
	| 1dmVU08zVpA  | 16087899  |
	| RB-wUgnyGv0  | 15712924  |
	| QjA5faZF1A8  | 15256922  |
	| -_CSo1gOd48  | 13199833  |
	| 49IDp76kjPw  | 11970018  |
	| tYnn51C3X_w  | 11823701  |
	| pv5zWaTEVkI  | 11672017  |
	| D2kJZOfq7zk  | 11184051  |
	+--------------+-----------+

2. 统计视频类别热度Top10

select t.category,
count(t.vedio_id) as hot
from 
	(select vedio_id,
	tbl.cate as category
	from 
	vedio_orc
	lateral view explode(category) tbl as cate) t
group by t.category
order by hot desc 
limit 10;

+----------------+---------+
|   t.category   |   hot   |
+----------------+---------+
| Music          | 179049  |
| Entertainment  | 127674  |
| Comedy         | 87818   |
| Animation      | 73293   |
| Film           | 73293   |
| Sports         | 67329   |
| Games          | 59817   |
| Gadgets        | 59817   |
| People         | 48890   |
| Blogs          | 48890   |
+----------------+---------+


3.统计出视频观看数最高的20个视频的所属类别以及类别包含Top20视频的个数

-- 1.首先统计观看次数最高的20个视频及其所属的类别

select vedio_id,
category
from vedio_orc
order by views desc 
limit 20; 

+--------------+---------------------+
|   vedio_id   |      category       |
+--------------+---------------------+
| dMH0bHeiRNg  | ["Comedy"]          |
| 0XxI-hvPRRA  | ["Comedy"]          |
| 1dmVU08zVpA  | ["Entertainment"]   |
| RB-wUgnyGv0  | ["Entertainment"]   |
| QjA5faZF1A8  | ["Music"]           |
| -_CSo1gOd48  | ["People","Blogs"]  |
| 49IDp76kjPw  | ["Comedy"]          |
| tYnn51C3X_w  | ["Music"]           |
| pv5zWaTEVkI  | ["Music"]           |
| D2kJZOfq7zk  | ["People","Blogs"]  |
| vr3x_RRJdd4  | ["Entertainment"]   |
| lsO6D1rwrKc  | ["Entertainment"]   |
| 5P6UU6m3cqk  | ["Comedy"]          |
| 8bbTtPL1jRs  | ["Music"]           |
| _BuRwH59oAo  | ["Comedy"]          |
| aRNzWyD7C9o  | ["UNA"]             |
| UMf40daefsI  | ["Music"]           |
| ixsZy2425eY  | ["Entertainment"]   |
| MNxwAU_xAMk  | ["Comedy"]          |
| RUCZJVJ_M8o  | ["Entertainment"]   |
+--------------+---------------------+

-- 2.把这20条信息中的category分裂出来(列转行)

select tbl.cate, t.vedio_id
from 
(select vedio_id,
category,
views
from vedio_orc
order by views desc 
limit 20) t 
lateral view explode(t.category) tbl as cate;

+----------------+--------------+
|    tbl.cate    |  t.vedio_id  |
+----------------+--------------+
| Comedy         | dMH0bHeiRNg  |
| Comedy         | 0XxI-hvPRRA  |
| Entertainment  | 1dmVU08zVpA  |
| Entertainment  | RB-wUgnyGv0  |
| Music          | QjA5faZF1A8  |
| People         | -_CSo1gOd48  |
| Blogs          | -_CSo1gOd48  |
| Comedy         | 49IDp76kjPw  |
| Music          | tYnn51C3X_w  |
| Music          | pv5zWaTEVkI  |
| People         | D2kJZOfq7zk  |
| Blogs          | D2kJZOfq7zk  |
| Entertainment  | vr3x_RRJdd4  |
| Entertainment  | lsO6D1rwrKc  |
| Comedy         | 5P6UU6m3cqk  |
| Music          | 8bbTtPL1jRs  |
| Comedy         | _BuRwH59oAo  |
| UNA            | aRNzWyD7C9o  |
| Music          | UMf40daefsI  |
| Entertainment  | ixsZy2425eY  |
| Comedy         | MNxwAU_xAMk  |
| Entertainment  | RUCZJVJ_M8o  |
+----------------+--------------+

-- 3.最后查询视频分类名称和该分类下有多少个Top20的视频

select t2.cate as category,
count(t2.vedio_id) as hot_as_view
from 
	(select tbl.cate as cate, t.vedio_id as vedio_id
	from 
		(select vedio_id,
		category,
		views
		from vedio_orc
		order by views desc 
		limit 20) t 
	lateral view explode(t.category) tbl as cate) t2 
group by t2.cate
order by  hot_as_view desc;

+----------------+--------------+
|    category    | hot_as_view  |
+----------------+--------------+
| Entertainment  | 6            |
| Comedy         | 6            |
| Music          | 5            |
| People         | 2            |
| Blogs          | 2            |
| UNA            | 1            |
+----------------+--------------+


4. 统计视频观看数Top50所关联视频的所属类别Rank


-- 1)查询出观看数最多的前50个视频的所有信息(当然包含了每个视频对应的关联视频)，记为临时表t
-- t：观看数前50的视频
select vedio_id,
relateId,
views
from vedio_orc
order by views desc 
limit 50;


-- 2)将找到的50条视频信息的相关视频relatedId列转行，记为临时表t2
-- t2：将相关视频的id进行列转行操作
select vedio_id,
tbl.rel
from 
	(select vedio_id,
	relateId,
	views
	from vedio_orc
	order by views desc 
	limit 50) t
lateral view explode(t.relateId) tbl as rel;


-- 3)将相关视频的id和video_orc表进行inner join操作
-- t5：得到两列数据，一列是之前查询出来的相关视频id,一列是category
select t2.relate_id, v.category
from 
	(select vedio_id,
	tbl.rel as relate_id
	from 
		(select vedio_id,
		relateId,
		views
		from vedio_orc
		order by views desc 
		limit 50) t
	lateral view explode(t.relateId) tbl as rel) t2 
join vedio_orc v 
on t2.relate_id = v.vedio_id;


+---------------+-----------------------+
 t2.relate_id  |      v.category       |
+---------------+-----------------------+
| er6QIKDtn8Y   | ["Film","Animation"]  |
| 4fODSPaqCfs   | ["Entertainment"]     |
| WMN1pPC1mcg   | ["Film","Animation"]  |
| SUbryU_XfX0   | ["Film","Animation"]  |
| ko-H5gX5C14   | ["Music"]             |
| bev149PYFZE   | ["Film","Animation"]  |
| 6wwcp0sNd7s   | ["Gadgets","Games"]   |
| eFsArrSXLZ8   | ["News","Politics"]   |
| moum7hC8mY8   | ["Film","Animation"]  |
| edxt2BqZErA   | ["Sports"]            |
| hjS5Q0KdHZI   | ["Film","Animation"]  |
| W7qA56yC64A   | ["Film","Animation"]  |
+---------------+-----------------------+


-- 4) 将t3表中的category炸开，进行行转列的操作

select tbl.cate, t3.relate_id
from 
	(select t2.relate_id, v.category
	from 
		(select vedio_id,
		tbl.rel as relate_id
		from 
			(select vedio_id,
			relateId,
			views
			from vedio_orc
			order by views desc 
			limit 50) t
	lateral view explode(t.relateId) tbl as rel) t2 
	join vedio_orc v 
	on t2.relate_id = v.vedio_id) t3 
lateral view explode(t3.category) tbl as cate;

+----------------+---------------+
|    tbl.cate    | t3.relate_id  |
+----------------+---------------+
| Politics       | ZZfBnemBats   |
| News           | vLl-2QG3HUs   |
| Politics       | vLl-2QG3HUs   |
| Travel         | 7WmMcqp670s   |
| Places         | 7WmMcqp670s   |
| People         | RFtTSisZtVY   |
| Blogs          | RFtTSisZtVY   |
| Travel         | JFeSH655mas   |
| Places         | JFeSH655mas   |
| Travel         | 6FzTEVnwYes   |
| Places         | 6FzTEVnwYes   |
+----------------+---------------+

-- 5) 按照视频类别进行分组，统计每组视频个数，然后排行

select t4.cate, count(t4.relate_id) as hot
from 
	(select tbl.cate, t3.relate_id
	from 
		(select t2.relate_id, v.category
		from 
			(select vedio_id,
			tbl.rel as relate_id
			from 
				(select vedio_id,
				relateId,
				views
				from vedio_orc
				order by views desc 
				limit 50) t
			lateral view explode(t.relateId) tbl as rel) t2 
		join vedio_orc v 
		on t2.relate_id = v.vedio_id) t3 
	lateral view explode(t3.category) tbl as cate) t4
group by t4.cate
order by hot desc;

+----------------+------+
|    t4.cate     | hot  |
+----------------+------+
| Comedy         | 237  |
| Entertainment  | 216  |
| Music          | 195  |
| Blogs          | 51   |
| People         | 51   |
| Film           | 47   |
| Animation      | 47   |
| News           | 24   |
| Politics       | 24   |
| Games          | 22   |
| Gadgets        | 22   |
| Sports         | 19   |
| Howto          | 14   |
| DIY            | 14   |
| UNA            | 13   |
| Places         | 12   |
| Travel         | 12   |
| Animals        | 11   |
| Pets           | 11   |
| Autos          | 4    |
| Vehicles       | 4    |
+----------------+------+


5. 统计每个类别中的视频热度Top10，以Music为例

-- 1) 要想统计Music类别中的视频热度Top10，需要先找到Music类别，那么就需要将category展开，
-- 所以可以创建一张表用于存放categoryId展开的数据。
create table if not exists vedio_category (
vedio_id string,
uploader string,
age int,
category string, 
length int,
views int,
rate float,
rating int,
comments string,
relateId array<string>)
row format delimited fields terminated by "\t"
collection items terminated by "&"
stored as orc
tblproperties("orc.compress"="SNAPPY");


-- 2) 向category展开的表中插入数据。
insert into table vedio_category
select vedio_id,
uploader,
age,
cate,
length,
views,
rate,
rating,
comments,
relateId 
from vedio_orc
lateral view explode(category) tbl as cate;


-- 3) 统计对应类别（Music）中的视频热度。

select vedio_id, views
from vedio_category
where category = "Music"
order by views desc
limit 10;

+--------------+-----------+
|   vedio_id   |   views   |
+--------------+-----------+
| QjA5faZF1A8  | 15256922  |
| tYnn51C3X_w  | 11823701  |
| pv5zWaTEVkI  | 11672017  |
| 8bbTtPL1jRs  | 9579911   |
| UMf40daefsI  | 7533070   |
| -xEzGIuY7kw  | 6946033   |
| d6C0bNDqf3Y  | 6935578   |
| HSoVKUVOnfQ  | 6193057   |
| 3URfWTEPmtE  | 5581171   |
| thtmaZnxk_0  | 5142238   |
+--------------+-----------+

6. 统计每个类别中视频流量Top10，以Music为例

select vedio_id, rating, views
from vedio_category
where category = "Music"
order by rating desc
limit 10;

+--------------+---------+-----------+
|   vedio_id   | rating  |   views   |
+--------------+---------+-----------+
| QjA5faZF1A8  | 120506  | 15256922  |
| pv5zWaTEVkI  | 42386   | 11672017  |
| UMf40daefsI  | 31886   | 7533070   |
| tYnn51C3X_w  | 29479   | 11823701  |
| 59ZX5qdIEB0  | 21481   | 1814798   |
| FLn45-7Pn2Y  | 21249   | 3604114   |
| -xEzGIuY7kw  | 20828   | 6946033   |
| HSoVKUVOnfQ  | 19803   | 6193057   |
| ARHyRI9_NB4  | 19243   | 1237802   |
| gg5_mlQOsUQ  | 19190   | 2595278   |
+--------------+---------+-----------+


7. 统计上传视频最多的用户Top10以及他们上传的观看次数在前20的视频

-- 上传视频最多的top10用户
select uploader, vedios
from user_orc
order by vedios desc
limit 10;

+---------------------+---------+
|      uploader       | vedios  |
+---------------------+---------+
| expertvillage       | 86228   |
| TourFactory         | 49078   |
| myHotelVideo        | 33506   |
| AlexanderRodchenko  | 24315   |
| VHTStudios          | 20230   |
| ephemeral8          | 19498   |
| HSN                 | 15371   |
| rattanakorn         | 12637   |
| Ruchaneewan         | 10059   |
| futifu              | 9668    |
+---------------------+---------+


-- 上传用户与vedio_orc join,找出这些用户上传的视频，并按照热度排名
select t1.uploader, 
v.vedio_id,
rank() over(partition by t1.uploader order by v.views) hot
from 
(select uploader, vedios
from user_orc
order by vedios desc
limit 10) t1 
left join vedio_orc v 
on t1.uploader = v.uploader;

-- 求出top20

select
	t2.uploader,
	t2.vedio_id,
	t2.hot
from
	(
	select
		t1.uploader,
		v.vedio_id,
		rank() over(partition by t1.uploader
	order by
		v.views) hot
	from
		(
		select
			uploader,
			vedios
		from
			user_orc
		order by
			vedios desc limit 10) t1
	left join vedio_orc v on
		t1.uploader = v.uploader) t2
where
	t2.hot <= 20;

	+---------------------+--------------+---------+
|     t2.uploader     | t2.vedio_id  | t2.hot  |
+---------------------+--------------+---------+
| AlexanderRodchenko  | NULL         | 1       |
| HSN                 | NULL         | 1       |
| Ruchaneewan         | GyyZLkd4ZDU  | 1       |
| Ruchaneewan         | nKxRzjKcxM0  | 2       |
| Ruchaneewan         | lw9tbm7es6Y  | 3       |
| Ruchaneewan         | dOlfPsFSjw0  | 4       |
| Ruchaneewan         | TmYbGQaRcNM  | 5       |
| Ruchaneewan         | qCfuQA6N4K0  | 6       |
| Ruchaneewan         | 4dkKeIUkN7E  | 7       |
| Ruchaneewan         | xbYyjUdhtJw  | 8       |
| Ruchaneewan         | DDl2cjI-aJs  | 9       |
| Ruchaneewan         | _RF_3VhaQpw  | 10      |
| Ruchaneewan         | lyUJB2eMVVg  | 11      |
| Ruchaneewan         | q4y2ZS5OQ88  | 12      |
| Ruchaneewan         | O3aoL70DlVc  | 13      |
| Ruchaneewan         | fGBVShTsuyo  | 14      |
| Ruchaneewan         | JgyOlXjjuw0  | 15      |
| Ruchaneewan         | Iq4e3SopjxQ  | 16      |
| Ruchaneewan         | 3hzOiFP-5so  | 16      |
| Ruchaneewan         | wenI5MrYT20  | 18      |
| Ruchaneewan         | 5Zf0lbAdJP0  | 19      |
| Ruchaneewan         | OwnEtde9_Co  | 20      |
| TourFactory         | NULL         | 1       |
| VHTStudios          | NULL         | 1       |
| ephemeral8          | NULL         | 1       |
| expertvillage       | 592QTdw3DOg  | 1       |
| expertvillage       | 3KXOnH_B5G0  | 2       |
| expertvillage       | gXlJg_1not4  | 3       |
| expertvillage       | pnNYbTo_-dY  | 4       |
| expertvillage       | 0_FAwlN4YSs  | 5       |
| expertvillage       | CKq7z8OKfnI  | 6       |
| expertvillage       | FDsmlxAm3pg  | 7       |
| expertvillage       | PaPMpO-f1eU  | 8       |
| expertvillage       | OBT3oPDFidM  | 9       |
| expertvillage       | fKGi2JOi5dI  | 10      |
| expertvillage       | ZdLowCydQwI  | 11      |
| expertvillage       | EaOtkJdxqvg  | 12      |
| expertvillage       | o7z02pYoI9g  | 13      |
| expertvillage       | 7-g8RIMo41k  | 14      |
| expertvillage       | k88mOlfBego  | 15      |
| expertvillage       | Bo06hwZZPSs  | 16      |
| expertvillage       | S9EgdPvRsAY  | 17      |
| expertvillage       | kLF38NlSYYE  | 18      |
| expertvillage       | 1mKOM0b6MwU  | 19      |
| expertvillage       | 6-NxJEbdcPE  | 20      |
| futifu              | NULL         | 1       |
| myHotelVideo        | NULL         | 1       |
| rattanakorn         | NULL         | 1       |
+---------------------+--------------+---------+

8.统计每个类别视频观看数Top10

-- 1. 从video_category表查出每个类别视频观看数排名
select
	category,
	vedio_id,
	rank() over(partition by category
order by
	views desc) hot
from
	vedio_category;


-- 2.取每个类别的Top10
select
	t.category,
	t.vedio_id,
	t.hot
from
	(
	select
		category,
		vedio_id,
		rank() over(partition by category
	order by
		views desc) hot
	from
		vedio_category) t
where
	t.hot <= 10;

+----------------+--------------+--------+
|   t.category   |  t.vedio_id  | t.hot  |
+----------------+--------------+--------+
| Animals        | 2GWPOPSXGYI  | 1      |
| Animals        | xmsV9R8FsDA  | 2      |
| Animals        | 12PsUW-8ge4  | 3      |
| Animals        | OeNggIGSKH8  | 4      |
| Animals        | WofFb_eOxxA  | 5      |
| Animals        | AgEmZ39EtFk  | 6      |
| Animals        | a-gW3RbJd8U  | 7      |
| Animals        | 8CL2hetqpfg  | 8      |
| Animals        | QmroaYVD_so  | 9      |
| Animals        | Sg9x5mUjbH8  | 10     |
| Animation      | sdUUx5FdySs  | 1      |
| Animation      | 6B26asyGKDo  | 2      |
| Animation      | H20dhY01Xjk  | 3      |
| Animation      | 55YYaJIrmzo  | 4      |
| Animation      | JzqumbhfxRo  | 5      |
| Animation      | eAhfZUZiwSE  | 6      |
| Animation      | h7svw0m-wO0  | 7      |
| Animation      | tAq3hWBlalU  | 8      |
| Animation      | AJzU3NjDikY  | 9      |
| Animation      | ElrldD02if0  | 10     |
+----------------+--------------+--------+
