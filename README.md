# 实验四 实验报告

181830187 王宇烽

* [x] 12.06更新：使用数据集的局部做测试，作业1～3程序做完，Debug结束

> *注：为了减少实验数据量大对于跑程序和Debug速度的影响，我选用了user_log_format1.csv中的前部分数据作为测试（user_log_format1_little.csv），后续程序测试准确无误再使用全部数据集进行计算。*

---

**代码结构：**

> 原始需求
> 
> > 实验四.pdf
> 
> 源码
> 
> > hadoop-tm
> > 
> > > pom.xml
> > > src（JAVA源代码）
> > > target
> > > user_info_format1.csv
> > 
> > py-tianmao
> > 
> > > q2.py（第一部分的spark部分和第二部分）
> > > q3.py（第三部分）
> > > spark-warehouse（空）
> 
> 运行结果
> 
> > hadoop打印输出
> > 
> > > age_30_top_merchant_result.csv 最受年轻人(age<30)关注的商家
> > > hot_item_result.csv 双十一最热门的商品
> > > merchant_count_result.csv 商家被关注度统计结果
> > 
> > spark输出
> > 
> > > age_30_top_merchant_result.csv 最受年轻人(age<30)关注的商家
> > > hot_item_result.csv 双十一最热门的商品
> > > merchant_count_result.csv 商家被关注度统计结果
> > > age_result.csv 统计购买了商品的买家年龄段的比例
> > > boy_girl_result.csv 统计双十一购买了商品的男女比例

---



一、分别编写MapReduce程序和Spark程序统计双十一最热门的商品和最受年轻人(age<30)关注的商家(“添加购物 +购买+添加收藏夹”前100名)

1. MapReduce程序设计

* 统计双十一最热门的商品

```java
设计思路：一个MapReduce完成
参考源码中的HotItemMapper.java 和 HotItemReducer.java
```

* 最受年轻人(age<30)关注的商家

```java
设计思路：两个MapReduce完成 

第一个MapReduce用于统计各个商家的受关注程度
参考源码中的MerchantTotalCountMapper.java和MerchantTotalCountReducer.java

第二个MapReduce用于排序
参考源码中的HotItemMapper.java 和 HotItemReducer.java
```

2. Spark程序设计

* 统计双十一最热门的商品

```python
设计思路：使用pyspark设计
参考源码中的q2.py 
hot_item = shop_rdd \
.filter(lambda shopBehavior: shopBehavior[6] != '0') \
.map(lambda shopBehavior: (shopBehavior[1], 1)) \
.reduceByKey(add) \
.sortBy(lambda x: x[1], False).take(100)
print ('============最热门的商品top100==============')
save_hot_item(hot_item,'/tmp/tianmao/hot_item_result.csv')
```

* 最受年轻人(age<30)关注的商家

```python
设计思路：使用pyspark设计
参考源码中的q2.py 
top100_merchant = shop_rdd \
.filter(lambda shopBehavior:shopBehavior[6] != "0") \
.filter(lambda shopBehavior:filter_age_30(shopBehavior,user_map)) \
.map(lambda shopBehavior:(shopBehavior[3], 1)) \
.reduceByKey(add) \
.sortBy(lambda x: x[1], False).take(100)
print('============30 age_range top 100 merchant==============')
save_30_age_top100_merchant(top100_merchant,'/tmp/tianmao/age_30_top_merchant_result.csv')
```

---



二. 编写Spark程序统计双十一购买了商品的男女比例，以及购买了商品的买家年龄段的比例；

* 统计双十一购买了商品的男女比例

```python
设计思路：使用pyspark设计
参考源码中的q2.py 
boy_girl_result = shop_rdd \
.filter(lambda shopBehavior:shopBehavior[6] == "2") \
.filter(lambda shopBehavior: filter_gender(shopBehavior, user_map)) \
.map(lambda shopBehavior:(user_map[shopBehavior[0]][2],1)) \
.reduceByKey(add) \
.collect()
boy_girl_result_count = 0.0
for ele in boy_girl_result:
boy_girl_result_count = boy_girl_result_count + ele[1]
print("============购买了商品的男⼥⽐例t==============")
save_boy_girl_result(boy_girl_result,boy_girl_result_count,'/tmp/tianmao/boy_girl_result.csv')
```

* 统计购买了商品的买家年龄段的比例

```python
设计思路：使用pyspark设计
参考源码中的q2.py 
age_result = shop_rdd \
.filter(lambda x:x[6] == "2") \
.map(lambda shopBehavior:get_user_age(shopBehavior,user_map)) \
.filter(lambda x:x != None) \
.reduceByKey(add) \
.collect()
agg_result_count = 0.0
for ele in age_result:
agg_result_count = agg_result_count + ele[1]
print('============购买了商品的买家年龄段的比例==============')
save_age_result(age_result,agg_result_count,'/tmp/tianmaoage_result.csv')
```

---



三. 基于Hive或者Spark SQL查询双十一购买了商品的男女比例，以及购买了商品的买家年龄段的比例；

**使用Spark SQL**

* Spark SQL查询双十一购买了商品的男女比例

```python
设计思路：使用pyspark+Spark SQL设计
参考源码中的q3.py
from operator import add
from pyspark import SparkConf, SparkContext
import sys

from pyspark.sql import SparkSession

if __name__ == '__main__':
spark = SparkSession.builder.appName("tianmao_sql").master("local[*]").getOrCreate()
shop_df = load_shop_data(spark).cache()
user_df = load_user_map(spark).cache()

shop_df.createOrReplaceTempView('shop_data')
user_df.createOrReplaceTempView('user_info')

print("============购买了商品的男⼥⽐例t==============")
spark.sql('select gender,gender_count/sum(gender_count) over (partition  by 1) as percent from (select count(*)as gender_count,gender from shop_data join user_info on(user_info.user_id = shop_data.user_id) where '
'user_info.gender in (''1'',''0'')'
' and shop_data.action_type == ''2'' group by gender)data  ') \
.show(10)
```

* Spark SQL查询购买了商品的买家年龄段的比例

```python
设计思路：使用pyspark+Spark SQL设计
参考源码中的q3.py
from operator import add
from pyspark import SparkConf, SparkContext
import sys

from pyspark.sql import SparkSession

if __name__ == '__main__':
spark = SparkSession.builder.appName("tianmao_sql").master("local[*]").getOrCreate()
shop_df = load_shop_data(spark).cache()
user_df = load_user_map(spark).cache()

shop_df.createOrReplaceTempView('shop_data')
user_df.createOrReplaceTempView('user_info')

print('============购买了商品的买家年龄段的比例==============')
spark.sql(
'select age_range,age_count/sum(age_count) over (partition  by 1) as percent from (select count(*)as age_count,age_range from shop_data join user_info on(user_info.user_id = shop_data.user_id) where '
' shop_data.action_type == ''2'' group by age_range)data  ') \
.show(10)
```

