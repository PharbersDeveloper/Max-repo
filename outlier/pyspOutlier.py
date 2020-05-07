# coding=utf-8
import numpy as np
from cvxpy import *

from pyspark.sql import SparkSession
import time
from pyspark.sql.types import *
from phOutlierParameters import *
from phcityoutliertemp import max_outlier_city_loop_template
from phimsshrjob import max_outlier_ims_shr_job
#from photfactor_udf import max_outlier_factor
from phpnljob import max_outlier_pnl_job
from phreaddfjob import max_outlier_read_df
from phpfjob import max_outlier_poi_job
from phselectcity import max_outlier_select_city
from phtmpmodify import max_outlier_tmp_mod
from phunijoineia import max_outlier_eia_join_uni

from copy import deepcopy
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,DoubleType
from pyspark.sql.functions import pandas_udf, PandasUDFType


spark = SparkSession.builder \
    .master("yarn") \
    .appName("sparkOutlier") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instance", "2") \
    .config("spark.executor.memory", "2g") \
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
#spark.sparkContext.addPyFile("photfactor_udf.py")
spark.sparkContext.addPyFile("phCalMktUdf.py")
spark.sparkContext.addPyFile("phSetSchema.py")
spark.sparkContext.addPyFile("phOutlierParameters.py")
spark.sparkContext.addPyFile("phRename.py")

print doi
print cities

start_time = time.time()  # 记录程序开始运行时间

'''
    工作目录: 1.panel  2.universe  3.IMS v.s. MAX 
'''
[df_EIA, df_uni, df_seg_city, df_hos_city, df_ims_shr] = max_outlier_read_df(spark, uni_path, pnl_path, ims_path)
# TODO：缺基于产品去重复
# POI 处理
print prd_input
[df_EIA_res, df_EIA] = max_outlier_poi_job(spark, df_EIA, prd_input)
df_EIA_res.persist()
df_EIA.persist()
#df_EIA_res.show()
# 处理universe join EIA
df_EIA_res = max_outlier_eia_join_uni(spark, df_EIA_res, df_uni)
# 对福建，厦门，泉州，珠江三角的调整需要
[df_EIA_res, df_seg_city, df_hos_city] = max_outlier_tmp_mod(spark, df_EIA_res, df_seg_city, df_hos_city)
# df_EIA_res.persist()
# df_EIA_res.show()
# 对Panel医院的数据整理， 用户factor的计算
[df_pnl, df_pnl_mkt] = max_outlier_pnl_job(spark, df_EIA, df_uni, df_hos_city)
# ims 个城市产品市场份额
[df_ims_shr_res, df_cities] = max_outlier_ims_shr_job(spark, df_ims_shr, prd_input)
# 城市处理逻辑
#df_cities = max_outlier_select_city(spark, df_cities)

#cities = df_cities.drop("key").toPandas()["city"].to_numpy()
#df_cities.show()

# df_seg_city.persist()
# df_EIA_res.persist()
# df_seg_city.write.format("parquet") \
#     .mode("overwrite").save(u"hdfs://192.168.100.137/user/alfredyang/outlier/seg_city")
# df_EIA_res.write.format("parquet") \
#     .mode("overwrite").save(u"hdfs://192.168.100.137/user/alfredyang/outlier/EIA_res")
# df_pnl.write.format("parquet") \
#     .mode("overwrite").save(u"hdfs://192.168.100.137/user/alfredyang/outlier/pnl")
# df_ims_shr_res.write.format("parquet") \
#     .mode("overwrite").save(u"hdfs://192.168.100.137/user/alfredyang/outlier/ims")

#start_time = time.time()  # 记录程序开始运行时间

#cities = [u"北京市"]
# df_seg_city = spark.read.parquet(u"hdfs://192.168.100.137/user/alfredyang/outlier/seg_city")
# df_EIA_res = spark.read.parquet(u"hdfs://192.168.100.137/user/alfredyang/outlier/EIA_res")
# df_pnl = spark.read.parquet(u"hdfs://192.168.100.137/user/alfredyang/outlier/pnl")
# df_ims_shr_res = spark.read.parquet(u"hdfs://192.168.100.137/user/alfredyang/outlier/ims")

df_result = max_outlier_city_loop_template(spark, df_EIA_res, df_seg_city, cities)
df_result.show()

# df_result = spark.read.parquet(tmp_df_result_path)

df_pnl = df_pnl.withColumnRenamed("City", "city") \
    .withColumnRenamed("Sales_pnl_mkt", "sales_pnl_mkt") \
    .withColumnRenamed("POI", "poi") \
    .withColumnRenamed("Sales_pnl", "sales_pnl")

df_pnl_mkt = df_pnl_mkt.withColumnRenamed("City", "city") \
    .withColumnRenamed("Sales_pnl_mkt", "sales_pnl_mkt")
# df_pnl.show()

df_pnl_mkt = df_pnl_mkt.withColumnRenamed("City", "city") \
    .withColumnRenamed("Sales_pnl_mkt", "sales_pnl_mkt")
# df_pnl.show()

df_result = df_result.join(df_pnl, on=["city", "poi"], how="left") \
    .join(df_pnl_mkt, on=["city"], how="left") \
    .join(df_ims_shr_res, on=["city", "poi"], how="left")
# df_result = df_result.join(df_pnl, on=["city", "poi"], how="left") \
#     .join(df_ims_shr_res, on=["city", "poi"], how="left")

# df_result.write.format("parquet") \
#         .mode("overwrite").save(u"hdfs://192.168.100.137/user/alfredyang/outlier/result")

#
# # 调试Factor 流程
# # TODO: 可以直接从这里开始调试factor
# cities = [u"北京市"]
# df_result = spark.read.parquet(u"hdfs://192.168.100.137/user/alfredyang/outlier/result")

df_result=df_result.withColumn("scen", df_result["scen"].cast(StringType()))
df_result = df_result.repartition(2)
df_result.write.format("parquet") \
    .mode("overwrite").save(df_result_path)
import os    
os.system("python photfactor_udf.py")
'''
[df_factor_result, df_rlt_brf] = max_outlier_factor(spark, df_result) 
df_factor_result = df_factor_result.repartition(2)
df_rlt_brf = df_rlt_brf.repartition(2)
df_factor_result.write.format("parquet") \
    .mode("overwrite").save(df_factor_result_path)
df_rlt_brf.write.format("parquet") \
    .mode("overwrite").save(df_rlt_brf_path)
''' 
end_time = time.time()  # 记录程序结束运行时间

print('Took %f second' % (end_time - start_time))
    
#test
#df_pandas=df_factor_result.toPandas()
#df_pandas.to_csv("./test/df_factor_result_test_udf.csv",encoding = 'utf_8_sig')
