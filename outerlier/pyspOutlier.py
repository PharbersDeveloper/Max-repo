# coding=utf-8
import itertools
import pandas as pd
import numpy as np
from cvxpy import *
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import *

from phcityoutliertemp import max_outlier_city_loop_template
from phimsshrjob import max_outlier_ims_shr_job
from phpnljob import max_outlier_pnl_job
from phreaddfjob import max_outlier_read_df
from phpfjob import max_outlier_poi_job
from phselectcity import max_outlier_select_city
from phtmpmodify import max_outlier_tmp_mod
from phunijoineia import max_outlier_eia_join_uni
from phsegwoot import max_outlier_seg_wo_ot_old, max_outlier_seg_wo_ot_spark

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
spark.sparkContext.addPyFile("phsegwoot.py")
spark.sparkContext.addPyFile("phcityoutliertemp.py")

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

cities = df_cities.drop("key").toPandas()["city"].to_numpy()
# df_cities.show()

df_seg_city.persist()
df_EIA_res.persist()

df_result = max_outlier_city_loop_template(spark, df_EIA_res, df_seg_city, cities)

df_pnl = df_pnl.withColumnRenamed("City", "city") \
    .withColumnRenamed("Sales_pnl_mkt", "sales_pnl_mkt") \
    .withColumnRenamed("POI", "poi") \
    .withColumnRenamed("Sales_pnl", "sales_pnl")
# df_pnl.show()

df_result = df_result.join(df_pnl, on=["city", "poi"], how="left") \
    .join(df_ims_shr_res, on=["city", "poi"], how="left")

df_result.show()

# 调试Factor 流程
'''
    @fst_prd: 计算factor时，仅考虑前几个产品
    @bias: 计算factor时，偏重于调准mkt的程度（数字越大越准）
'''
fst_prd = 3
bias = 2
prd_input = ["加罗宁", "凯纷", "诺扬"]
for ct in cities:
    print u"正在进行 %s factor的计算" % ct
    df_rlt = df_result.where(df_result.city == ct)
    rlt_scs = df_rlt.select("scen_id").distinct().toPandas()["scen_id"].to_numpy().tolist()

    for isc in rlt_scs:
        df_rlt_sc = df_rlt.where(df_rlt.scen_id == isc).fillna(0.0)
        df_rlt_sc.show()
        # 一个线下算法库，没有替代品的情况下线下计算
        rltsc = df_rlt_sc.toPandas()
        f = Variable()
        poi_ratio = {}
        mkt_ratio = {}

        for iprd in range(len(rltsc.index)):
            poi_ratio[iprd] = np.divide(
                (rltsc["poi_vol"][iprd] - rltsc["sales_pnl"][iprd]) * f + rltsc["sales_pnl"][iprd],
                rltsc["ims_poi_vol"][iprd]) - 1
            mkt_ratio[iprd] = np.divide(
                (rltsc["mkt_vol"][iprd] - rltsc["sales_pnl_mkt"][iprd]) * f + rltsc["sales_pnl_mkt"][iprd],
                rltsc["ims_mkt_vol"][iprd]) - 1

        par = []
        for s in range(len(rltsc.index)):
            if rltsc["poi"][s] in prd_input[:fst_prd]:
                par += ["np.divide(abs(poi_ratio[%s])," % s + str(bias) + ")"]
                par += ["abs(mkt_ratio[%s])" % s]
        exec ("obj=Minimize(max_elemwise(" + ",".join(par) + "))")
        #        obj=Minimize(max_elemwise(abs(poi_ratio[0]),abs(poi_ratio[1]),abs(poi_ratio[2]),abs(poi_ratio[3]),
        #                                  abs(mkt_ratio[0]),abs(mkt_ratio[1]),abs(mkt_ratio[2]),abs(mkt_ratio[3])))
        prob = Problem(obj, [f > 0])
        prob.solve()
        print f
        # df_rlt_sc = spark.createDataFrame(rltsc)
