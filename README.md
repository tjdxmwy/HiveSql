# 1 HiveSql01

### 1.1 库的DDL

### 1.2 表的DDL

### 1.3 外部表和内部表

### 1.4 分区表

### 1.5 DML



# 2 HiveSql02

### 2.1 基本查询

​	全表查询，where，函数

### 2.2 条件过滤

### 2.3 分组

### 2.4 连接

### 2.5 排序

### 2.6 分桶



# 3 HiveSql03

### 3.1 常用函数

##### 	3.1.1 NVL

##### 	3.1.2  case when

##### 	3.1.3 行转列(concat、concat_ws、collect_list、collect_set)

##### 	3.1.4 列转行(EXPLODE、LATERAL VIEW)

### 3.2  窗口函数

##### 	3.2.1  sum(...) over(partition by ... order by ...)

##### 	3.2.2  lag(...) over(...)

##### 		   lead(...) over(...)

##### 	3.2.3  ntile(5) over(...)

##### 	3.2.4  percent_rank() over(...)

##### 	3.2.5  rank, dense_rank, row_number

### 3.3  日期函数

- ##### current_date 

- ##### date_add

- ##### date_format

- ##### date_sub

- ##### datediff

- ##### to_date 



# 4 HiveSql04

### 4.1 用户自定义函数

##### 	4.1.1 自定义函数

```markdown
1）Hive 自带了一些函数，比如：max/min等，但是数量有限，自己可以通过自定义UDF来方便的扩展。
2）当Hive提供的内置函数无法满足你的业务处理需要时，此时就可以考虑使用用户自定义函数（UDF：user-defined function）。
3）根据用户自定义函数类别分为以下三种：
    （1）UDF（User-Defined-Function）
        一进一出
    （2）UDAF（User-Defined Aggregation Function）
        聚集函数，多进一出
        类似于：count/max/min
    （3）UDTF（User-Defined Table-Generating Functions）
        一进多出
        如lateral view explore()
```

### 4.2 存储和压缩对比

### 4.3 表的优化



# 5 HiveSql05