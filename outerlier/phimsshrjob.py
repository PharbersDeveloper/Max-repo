# coding=utf-8
from pyspark.sql.types import *
from pyspark.sql import functions as func


# 6. ims 个城市产品市场份额


def gen_poi_with_input(prds):
    result = []
    for it in prds:
        result.append((1, it))
    return result


def max_outlier_ims_shr_job(spark, df_ims_shr):
    df_ims_shr = df_ims_shr.where(
        (df_ims_shr.city != "CHPA")
        & (df_ims_shr.city == "北京市")
        # & (df_ims_shr.city == "宁波市")
        # & (df_ims_shr.city == "珠三角市")
    )
    df_cities = df_ims_shr.select("city").distinct().withColumn("key", func.lit(1))

    prd_input = [u"加罗宁", u"凯纷", u"诺扬"]
    schema = StructType([StructField("key", IntegerType(), True), StructField("poi", StringType(), True)])
    df_tmp_poi = spark.createDataFrame(gen_poi_with_input(prd_input), schema)

    df_ct_pd = df_cities.join(df_tmp_poi, on="key", how="outer") \
        .withColumn("ims_share", func.lit(0)) \
        .withColumn("ims_poi_vol", func.lit(0)) \
        .drop("key")
    # df_ct_pd.show()

    df_ims_shr_mkt = df_ims_shr.groupBy("city").sum("ims_poi_vol").withColumnRenamed("sum(ims_poi_vol)", "ims_mkt_vol")
    df_ims_shr_poi = df_ims_shr.union(df_ct_pd)
    df_ims_shr_res = df_ims_shr_mkt.join(df_ims_shr_poi, on="city").groupby(["city", "poi"]) \
        .agg({"ims_share": "sum", "ims_poi_vol": "sum", "ims_mkt_vol": "first"}) \
        .withColumnRenamed("sum(ims_poi_vol)", "ims_poi_vol") \
        .withColumnRenamed("sum(ims_share)", "ims_share") \
        .withColumnRenamed("first(ims_mkt_vol)", "ims_mkt_vol")

    return [df_ims_shr_res, df_cities]
