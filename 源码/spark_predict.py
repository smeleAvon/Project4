# -- coding:utf-8 --


from pyspark import SparkConf, SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.classification import SVMWithSGD
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.feature import StandardScaler
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


def load_train_data(sc):
    return sc \
        .textFile('/data_format1/train_format1_l.c') \
        .map(lambda x: x.split(',')) \
        .filter(lambda x: len(x) == 3 and x[0] != 'user_id' and x[2] == '0' or x[2] == '1')


def load_test_data(sc):
    return sc \
        .textFile('/data_format1/test_format1.csv') \
        .map(lambda x: x.split(',')) \
        .filter(lambda x: len(x) == 3 and x[0] != 'user_id')


def fill_user_info(user_info):
    if (user_info[1] == None or user_info[1] == ''):
        user_info[1] = '-1'
    if (user_info[2] == None or user_info[2] == ''):
        user_info[2] = '-1'
    return user_info


def load_user_map(sc):
    return sc \
        .textFile('/data_format1/user_info_format1.csv') \
        .map(lambda x: x.split(',')) \
        .filter(lambda x: len(x) == 3 and x[0] != 'user_id') \
        .map(fill_user_info) \
        .keyBy(lambda x: x[0]) \
        .collectAsMap()


def set_user_info(user_data, user_map, set_lable):
    user_id = user_data[0]
    user_info = user_map[user_id]
    if (user_info == None):
        return None
    else:
        # user_id  merchant_id age_range gender label
        if (set_lable):
            return (user_data[0], user_data[1], user_info[1], user_info[2], user_data[2])
        else:
            return (user_data[0], user_data[1], user_info[1], user_info[2])


def build(user):
    return LabeledPoint(float(user[0]), Vectors.dense(user[1]))


def build_vectors(data_user_info):
    return data_user_info \
        .map(lambda user: Vectors.dense(user))


def reverse_vectors(vertors):
    values = vertors.values
    return (values[0], values[1])


def reverse_label_point(lable_point):
    return ((lable_point.features.values[0], lable_point.features.values[1]), lable_point.label)


def set_train_user_info(train_data, user_map):
    return train_data \
        .map(lambda user_data: set_user_info(user_data, user_map, True)) \
        .filter(lambda user_data: user_data != None)


def set_test_user_info(test_data, user_map):
    return test_data.map(lambda user_data: set_user_info(user_data, user_map, False)) \
        .filter(lambda user_data: user_data != None)


def build_point(data_user_info):
    return data_user_info \
        .map(lambda user: build(user))


if __name__ == '__main__':

    conf = SparkConf().setMaster("local[*]").setAppName("tianmao")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("warn")
    user_map = load_user_map(sc)
    # 加载训练数据
    train_data = load_train_data(sc)
    # 设置数据的用户信息数据
    train_data_user_info = set_train_user_info(train_data, user_map)
    # user_id  merchant_id age_range gender label
    train_data_user_info.cache()
    stand_train_data_user_info = train_data_user_info.map(lambda user: user[0:4])
    stand_train_data_user_info_label = train_data_user_info.map(lambda user: user[4])

    #训练数据标准化
    std_scaler = StandardScaler(True, True).fit(stand_train_data_user_info)
    stand_train_data_user_info = std_scaler.transform(stand_train_data_user_info)

    train_data_user_info = stand_train_data_user_info_label.zip(stand_train_data_user_info)
    # 构建标签数据
    train_data_user_info = build_point(train_data_user_info)
    numIterations = 100

    train_data_user_info.cache()
    #训练模型
    model = SVMWithSGD.train(train_data_user_info, numIterations)
    #model = DecisionTree.trainClassifier(train_data_user_info,numIterations,2,{})

    # 加载测试数据
    test_data = load_test_data(sc)
    # 设置数据的用户信息数据
    test_data_user_info = set_test_user_info(test_data, user_map)
    test_data_user_info.cache()

    # 测试数据标准化
    std_scaler = StandardScaler(True, True).fit(test_data_user_info)
    stand_test_data_user_info = std_scaler.transform(test_data_user_info)

    # 构建标签数据
    test_data_user_info = test_data_user_info.map(lambda user: user[0:2]).zip(build_vectors(stand_test_data_user_info))

    # 进行预测
    #对test数据进行预测
    predict_data = test_data_user_info.map(lambda user: ((user[0][0], user[0][1]), model.predict(user[1])))
    #对训练集数据进行预测
    predict_self_data = train_data_user_info.map(
        lambda x: (x.features.values[0], x.features.values[1], x.label, model.predict(x.features)))

    # 计算准确率
    total_num = predict_self_data.count()
    correct_num = predict_self_data.filter(lambda x: float(x[2]) == float(x[3])).count()
    print("准确率:%s" % (correct_num / float(total_num)))

    result = predict_data.collect()
    with open('/data_format1/predict.csv', 'w') as file:
        for data in result:
            content = "用户id:%s,商家id:%s,预测是否为回头客:%s" % (data[0][0], data[0][1], float(data[1]) == float(1))
            print(content)
            file.write(content)
            file.write('\n')
