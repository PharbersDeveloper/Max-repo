# coding=utf-8
from pyspark.sql.functions import udf
from pyspark.sql.types import *


def udf_city_modi(city):
    if city in [u"福州市",u"厦门市",u"泉州市"]:
        return u"福厦泉市"
    elif city in [u"珠海市",u"东莞市",u"中山市",u"佛山市"]:
        return u"珠三角市"
    else:
        return city


def max_outlier_tmp_mod(spark, df_EIA, df_seg_city, df_hos_city):
    max_outlier_city_udf = udf(udf_city_modi, StringType())
    df_EIA = df_EIA.withColumn("City", max_outlier_city_udf(df_EIA.City))
    df_seg_city = df_seg_city.withColumn("City", max_outlier_city_udf(df_seg_city.City))
    df_hos_city = df_hos_city.withColumn("City", max_outlier_city_udf(df_hos_city.City))
    return [df_EIA, df_seg_city, df_hos_city]

