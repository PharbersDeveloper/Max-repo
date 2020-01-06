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


def max_outlier_tmp_mod(spark, df_EIA):
    max_outlier_city_udf = udf(udf_city_modi, StringType())
    df_EIA = df_EIA.withColumn("City", max_outlier_city_udf(df_EIA.City))
    return df_EIA

