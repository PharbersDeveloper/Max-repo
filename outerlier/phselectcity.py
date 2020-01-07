# coding=utf-8
from pyspark.sql.functions import udf
from pyspark.sql.types import *


'''
    需要在list中选择 需要挑选outlier 的城市
'''


def max_outlier_select_city(spark, df_cities):
    # lst=[u'北京市',u'常州市',u'福厦泉市',u'广州市',u'福厦泉市',u'广州市',u'宁波市',u'上海市',\
    #   u'苏州市',u'天津市',u'温州市',u'无锡市',u'西安市',u'郑州市',u'珠三角市']
    # cities = cities[cities.isin(lst)].reset_index(drop=True)
    # cities = cities.iloc[[2,3,5]].reset_index(drop=True)
    # TODO: 缺城市处理逻辑
    return df_cities



