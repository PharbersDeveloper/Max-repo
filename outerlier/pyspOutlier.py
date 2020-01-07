# coding=utf-8
import pandas as pd
import numpy as np
import sys
reload(sys)
sys.setdefaultencoding('utf8')

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import *

from phimsshrjob import max_outlier_ims_shr_job
from phpnljob import max_outlier_pnl_job
from phreaddfjob import max_outlier_read_df
from phpfjob import max_outlier_poi_job
from phselectcity import max_outlier_select_city
from phtmpmodify import max_outlier_tmp_mod
from phunijoineia import max_outlier_eia_join_uni

spark = SparkSession.builder \
    .master("yarn") \
    .appName("sparkOutlier") \
    .getOrCreate()

spark.sparkContext.addPyFile("phreaddfjob.py")
spark.sparkContext.addPyFile("phpfjob.py")
spark.sparkContext.addPyFile("phunijoineia.py")
spark.sparkContext.addPyFile("phtmpmodify.py")
spark.sparkContext.addPyFile("phpnljob.py")
spark.sparkContext.addPyFile("phimsshrjob.py")
spark.sparkContext.addPyFile("phselectcity.py")

'''
    工作目录: 1.panel  2.universe  3.IMS v.s. MAX 
'''
[df_EIA, df_uni, df_seg_city, df_hos_city, df_ims_shr] = max_outlier_read_df(spark)
# TODO：缺基于产品去重复
# POI 处理
[df_EIA_res, df_EIA] = max_outlier_poi_job(spark, df_EIA)
df_EIA_res.persist()
df_EIA.persist()
# 处理universe join EIA
df_EIA_res = max_outlier_eia_join_uni(spark, df_EIA_res, df_uni)
# 对福建，厦门，泉州，珠江三角的调整需要
[df_EIA_res, df_seg_city, df_hos_city] = max_outlier_tmp_mod(spark, df_EIA_res, df_seg_city, df_hos_city)
df_EIA_res.persist()
# df_EIA_res.show()
# 对Panel医院的数据整理， 用户factor的计算
df_pnl = max_outlier_pnl_job(spark, df_EIA, df_uni, df_hos_city)
# ims 个城市产品市场份额
[df_ims_shr_res, df_cities] = max_outlier_ims_shr_job(spark, df_ims_shr)
# 城市处理逻辑
df_cities = max_outlier_select_city(spark, df_cities)

# df_ims_shr_res.show()
# print df_ims_shr_res.count()

# cities = df_cities.toPandas()
# data_EIA4 = df_EIA_res.toPandas()

# cities = np.array(df_seg_city.toPandas()[:, "city"].toList())
# print cities

# 通过Seg来过滤数据
df_seg_city_tmp = df_seg_city.join(df_cities, df_seg_city.City == df_cities.city, how="inner").select("Seg").distinct()
df_seg_city_tmp.show()
df_EIA_res.show()
df_EIA_res = df_EIA_res.join(df_seg_city_tmp, on="Seg", how="inner")

data_EIA5 = df_EIA_res.toPandas()
print data_EIA5.head()
prd_input = ["加罗宁", "凯纷", "诺扬"]
data_EIA5["mkt_size"] = data_EIA5[prd_input].sum(axis=1).fillna(0)+data_EIA5["其它"].fillna(0)
print data_EIA5.head()
