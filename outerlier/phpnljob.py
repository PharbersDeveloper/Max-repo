# coding=utf-8
from pyspark.sql.types import *
import pandas as pd


def max_outlier_pnl_job(spark, df_EIA, df_uni, df_hos_city):
    df_panel_hos = df_uni.where(df_uni.PANEL == 1).select("HOSP_ID")
    df_pnl = df_EIA.join(df_hos_city, df_EIA.HOSP_ID == df_hos_city.Panel_ID, how="left")
    # df_pnl.show()

    df_pnl = df_pnl.groupBy(["City", "POI"]).sum("Sales").withColumnRenamed("sum(Sales)", "Sales")
    df_pnl.persist()
    # pnl = df_pnl.toPandas()
    # pnl = pnl.set_index(["City", "POI"])
    # pnl = pnl.unstack("POI").fillna(0).stack()
    #
    # pnl2= \
    #     pd.DataFrame(pnl.sum(level="City")).reset_index() \
    #         .merge(pd.DataFrame(pnl).reset_index(),
    #                on="City",
    #                how="right",
    #                suffixes=["_pnl_mkt","_pnl"])
    #
    # print pnl2

    df_pnl_tmp = df_pnl.withColumnRenamed("Sales", "Sales_pnl")
    df_pnl_mkt = df_pnl.groupBy("City").agg({"Sales": "sum"}) \
        .withColumnRenamed("sum(Sales)", "Sales_pnl_mkt")

    df_pnl = df_pnl_tmp.join(df_pnl_mkt, on=["City"], how="left")
    # print df_pnl.count()
    # df_pnl.where(df_pnl.City == "上海市").show()
    return df_pnl
