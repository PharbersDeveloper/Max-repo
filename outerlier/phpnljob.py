# coding=utf-8
from pyspark.sql.types import *


def max_outlier_pnl_job(spark, df_EIA, df_uni, df_hos_city):
    df_panel_hos = df_uni.where(df_uni.PANEL == 1).select("HOSP_ID")
    df_pnl = df_EIA.join(df_hos_city, df_EIA.HOSP_ID == df_hos_city.Panel_ID, how="left")
    # df_pnl.show()

    df_pnl = df_pnl.groupBy(["City", "POI"]).sum("Sales").withColumnRenamed("sum(Sales)", "Sales")
    df_pnl.persist()

    df_pnl.withColumnRenamed("POI", "POI_pnl") \
        .withColumnRenamed("Sales", "Sales_pnl")
    df_pnl_mkt = df_pnl.groupBy("City").agg({"POI": "first", "Sales": "sum"}) \
        .withColumnRenamed("first(POI)", "POI_pnl_mkt") \
        .withColumnRenamed("sum(Sales)", "Sales_pnl_mkt")

    df_pnl = df_pnl_mkt.join(df_pnl, on="City", how="right")
    return df_pnl
