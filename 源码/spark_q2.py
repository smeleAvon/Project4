# -- coding:utf-8 --
from operator import add
from pyspark import SparkConf, SparkContext
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def p(o):
    print o


def load_shop_data(sc):
    return sc \
        .textFile('/Users/data_format1/user_log_format1.csv') \
        .map(lambda x: x.split(',')) \
        .filter(lambda x: len(x) == 7 and x[5] == '1111') \
        .cache()


def load_user_map(sc):
    return sc \
        .textFile('/Users/data_format1/user_info_format1.csv') \
        .map(lambda x: x.split(',')) \
        .filter(lambda x: len(x) == 3) \
        .keyBy(lambda x: x[0]) \
        .collectAsMap()

def filter_age_30(shopBehavior,user_map):
    user_id = shopBehavior[0]
    user_info = user_map[user_id]
    if (user_info == None):
        return False
    else:
        age = user_info[1]
        return age == '3' or age == '2' or age == '1'

def filter_gender(shopBehavior,user_map):
    user_id = shopBehavior[0]
    user_info = user_map[user_id]
    if (user_info == None):
        return False
    else:
        gender = user_info[2]
        return gender == '0' or gender == '1'
def get_user_age(shopBehavior,user_map):
    user_id = shopBehavior[0]
    user_info = user_map[user_id]
    if (user_info == None):
        return None
    else:
        return (user_info[1],1)

def save_hot_item(hot_item,file):
    with open(file,'w') as f :
        for i in range(0, len(hot_item)):
            content = '第%s名的商品id:%s,购买量:%s\n' % (i+1,hot_item[i][0],hot_item[i][1])
            print content
            f.write(content)


def save_merchant_total_count(merchant_total_count,file):
    with open(file,'w') as f:
        for i in range(0, len(merchant_total_count)):
            content = '商家id:%s,出货量:%s\n' % (merchant_total_count[i][0], merchant_total_count[i][1])
            print content
            f.write(content)


def save_30_age_top100_merchant(top100_merchant, file):
    with open(file,'w') as f:
        for i in range(0, len(top100_merchant)):
            content = '第%s名的商家id:%s,出货量:%s\n' % (i+1,top100_merchant[i][0], top100_merchant[i][1])
            print content
            f.write(content)


def save_boy_girl_result(boy_girl_result,boy_girl_result_count, file):
    with open(file,'w') as f:
        for ele in boy_girl_result:
            content = '性别标识:%s,购物比例%s\n' % (ele[0],ele[1] / boy_girl_result_count)
            print content
            f.write(content)


def save_age_result(age_result, agg_result_count, file):
    with open(file,'w') as f:
        for ele in age_result:
            content = '年龄标识:%s,购物比例%s\n' % (ele[0], ele[1]/agg_result_count)
            print content
            f.write(content)


if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("tianmao")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("warn")

    # (user_id:0 item_id:1 cat_id:2 merchant_id:3 brand_id:4 time_stamp:5 action_type:6)
    shop_rdd = load_shop_data(sc)

    # (user_id:0,(user_id:0,age_range:1,gender:2))
    user_map = load_user_map(sc)

    # 统计双⼗⼀最热⻔的商品 item_id为商品id  把每一个商品上的所有顾客行为(除了点击)次数统计 输出前100
    hot_item = shop_rdd \
        .filter(lambda shopBehavior: shopBehavior[6] != '0') \
        .map(lambda shopBehavior: (shopBehavior[1], 1)) \
        .reduceByKey(add) \
        .sortBy(lambda x: x[1], False).take(100)
    print ('============最热门的商品top100==============')
    save_hot_item(hot_item,'/tmp/tianmao/hot_item_result.csv')

    merchant_total_count = shop_rdd \
      .filter(lambda shopBehavior:shopBehavior[6] != "0") \
      .map(lambda shopBehavior:(shopBehavior[3],1)) \
      .reduceByKey(add) \
      .collect()
    print('============每一个商家被光顾的记录==============')
    save_merchant_total_count(merchant_total_count,'/tmp/tianmao/merchant_count_result.csv')


    #统计最受年轻⼈(age < 30)关注的商家（“添加购物⻋+购买 + 添加收藏夹”前100名）
    #merchant_id为商家id 找出count最多的前100名

    top100_merchant = shop_rdd \
        .filter(lambda shopBehavior:shopBehavior[6] != "0") \
        .filter(lambda shopBehavior:filter_age_30(shopBehavior,user_map)) \
        .map(lambda shopBehavior:(shopBehavior[3], 1)) \
        .reduceByKey(add) \
        .sortBy(lambda x: x[1], False).take(100)
    print('============30 age_range top 100 merchant==============')
    save_30_age_top100_merchant(top100_merchant,'/tmp/tianmao/age_30_top_merchant_result.csv')


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



