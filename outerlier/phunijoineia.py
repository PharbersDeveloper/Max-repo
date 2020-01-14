# coding=utf-8
from pyspark.sql.types import *


def gen_date_with_year(year):
    result = []
    for month in range(12):
        result.append((1, year * 100 + month + 1))
    return result


def max_outlier_eia_join_uni(spark, df_EIA, df_uni):
    arg_year = 2018
    date = gen_date_with_year(arg_year)
    schema = StructType([StructField("key", IntegerType(), True), StructField("Date", IntegerType(), True)])
    date = spark.createDataFrame(date, schema)

    df_uni = df_uni.join(date, on="key", how="outer")

    df_EIA = df_uni.join(df_EIA, on=["HOSP_ID", "Date"], how="left")
    # print df_EIA.count()
    return df_EIA
