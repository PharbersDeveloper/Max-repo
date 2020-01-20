# coding=utf-8
from pyspark.sql import functions as func


def max_outlier_read_df(spark, uni_path, pnl_path, ims_path):
    # 1. panel 数据处理
    df_EIA = spark.read.parquet(pnl_path)
    df_EIA = df_EIA.withColumn("Year", func.bround(df_EIA.Date / 100))

    df_uni = spark.read.parquet(uni_path)
    df_seg_city = df_uni.select("City", "Seg").distinct()
    df_hos_city = df_uni.select("Panel_ID", "City").distinct()
    df_uni = df_uni.select("Panel_ID", "Seg", "City", "BEDSIZE", "Est_DrugIncome_RMB", "PANEL")
    df_uni = df_uni.withColumn("key", func.lit(1)).withColumnRenamed("Panel_ID", "HOSP_ID")
    df_uni.persist()

    df_ims_shr = spark.read.parquet(ims_path)\
        .select("city", "poi", "ims_share", "ims_poi_vol")

    # return {"EIA": df_EIA, "UNI": df_uni, "SEGCITY": df_seg_city, "HOSPCITY": df_hos_city}
    return [df_EIA, df_uni, df_seg_city, df_hos_city, df_ims_shr]
