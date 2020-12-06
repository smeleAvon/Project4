# coding:utf-8
from operator import add
from pyspark import SparkConf, SparkContext
import sys

from pyspark.sql import SparkSession


def p(o):
    print o


def load_shop_data(spark):
    return spark.read.option('header', True).csv(
        '/Users/data_format1/user_log_format1_little.csv')


def load_user_map(spark):
    return spark.read.option('header', True) \
        .csv('/Users/data_format1/user_info_format1.csv')

def filter_age_30(shopBehavior, user_map):
    user_id = shopBehavior[0]
    user_info = user_map[user_id]
    if (user_info == None):
        return False
    else:
        age = user_info[1]
        return age == '3' or age == '2' or age == '1'


def filter_gender(shopBehavior, user_map):
    user_id = shopBehavior[0]
    user_info = user_map[user_id]
    if (user_info == None):
        return False
    else:
        gender = user_info[2]
        return gender == '0' or gender == '1'


def get_user_age(shopBehavior, user_map):
    user_id = shopBehavior[0]
    user_info = user_map[user_id]
    if (user_info == None):
        return None
    else:
        return (user_info[1], 1)


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

    print('============购买了商品的买家年龄段的比例==============')
    spark.sql(
        'select age_range,age_count/sum(age_count) over (partition  by 1) as percent from (select count(*)as age_count,age_range from shop_data join user_info on(user_info.user_id = shop_data.user_id) where '
        ' shop_data.action_type == ''2'' group by age_range)data  ') \
        .show(10)
