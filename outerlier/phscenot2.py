# coding=utf-8
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark.sql import functions as func
import numpy as np
import pandas as pd


@udf(ArrayType(IntegerType()))
def rep_EIA():
    res = []
    for x in range(256):
        res.append(x)
    return res


@udf(BooleanType())
def is_scen(lst, cur):
    if isinstance(lst, list):
        if cur in lst:
            return True
        else:
            return False
    else:
        return False


def max_outlier_seg_scen_ot_spark_2(spark, df_EIA_res_cur,
                                    df_panel, ct, scen,
                                    ot_seg, other_seg_poi, other_seg_oth):

    # df_panel.show()
    # print df_panel.count()

    prd_input = ["加罗宁", "凯纷", "诺扬"]

    schema = StructType([
        StructField("scen", ArrayType(StringType()), True),
    ])
    arr = np.array(scen[ot_seg])
    df = pd.DataFrame(data=arr.flatten())
    df.columns = ["scen"]
    df_scen_ot_seg = spark.createDataFrame(df, schema)

    # 有问题,后期修改
    df_scen_ot_seg = df_scen_ot_seg.repartition(1).withColumn("scen_id", func.monotonically_increasing_id()) \
        .withColumn("num_ot", func.size("scen"))

    df_EIA_res_cur = df_EIA_res_cur.withColumn("scen_id_lst", rep_EIA())
    df_EIA_res_cur = df_EIA_res_cur.select("*", func.explode("scen_id_lst").alias("scen_id")).drop("scen_id_lst")

    df_EIA_res_cur = df_EIA_res_cur.join(df_scen_ot_seg, on=["scen_id"], how="left").fillna(0)
    # df_EIA_res_cur.show(1000)

    df_EIA_res_cur = df_EIA_res_cur.withColumn("is_bed_gt_100", df_EIA_res_cur.BEDSIZE >= 100) \
        .withColumn("is_panel", df_EIA_res_cur.PANEL == 1) \
        .withColumn("is_city", df_EIA_res_cur.City == ct) \
        .withColumn("is_scen", is_scen(df_EIA_res_cur.scen, df_EIA_res_cur.HOSP_ID))

    df_EIA_res_cur.persist()

    # 0. vol_ot
    df_EIA_res_cur_vol_ot = df_EIA_res_cur.where(df_EIA_res_cur.is_scen) \
        .groupBy("scen_id").sum("Est_DrugIncome_RMB") \
        .withColumnRenamed("sum(Est_DrugIncome_RMB)", "vol_ot")

    df_EIA_res_cur = df_EIA_res_cur.join(df_EIA_res_cur_vol_ot, on="scen_id", how="left").fillna(0)

    # 1. poi_ot & oth_ot
    df_EIA_res_cur_poi_ot = df_EIA_res_cur.where(
        df_EIA_res_cur.is_bed_gt_100 & df_EIA_res_cur.is_city & df_EIA_res_cur.is_scen)
    df_EIA_res_cur_poi_ot = df_EIA_res_cur_poi_ot.groupBy("scen_id").sum("加罗宁", "凯纷", "诺扬", "其它") \
        .withColumnRenamed("sum(加罗宁)", "加罗宁_poi_ot") \
        .withColumnRenamed("sum(凯纷)", "凯纷_poi_ot") \
        .withColumnRenamed("sum(诺扬)", "诺扬_poi_ot") \
        .withColumnRenamed("sum(其它)", "oth_ot").fillna(0.0)

    # 1.1  数据还需要补全
    df_scen_ot_seg.show(300)
    df_EIA_res_cur_poi_ot = df_scen_ot_seg.join(df_EIA_res_cur_poi_ot, on="scen_id", how="left").fillna(0)
    df_EIA_res_cur_poi_ot.show(300)
    # df_EIA_res_cur = df_EIA_res_cur.join(df_EIA_res_cur_poi_ot, on="scen_id", how="left").fillna(0)
    # df_EIA_res_cur.persist()

    # 2. rest
    df_EIA_res_rest = df_EIA_res_cur.where(~df_EIA_res_cur.is_scen)
    df_rest_seg = df_EIA_res_rest.groupBy("scen_id", "Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
        .withColumnRenamed("sum(加罗宁)", "加罗宁") \
        .withColumnRenamed("sum(凯纷)", "凯纷") \
        .withColumnRenamed("sum(诺扬)", "诺扬") \
        .withColumnRenamed("sum(其它)", "其它") \
        .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB").fillna(0.0)

    df_rest_seg_p0_bed100 = df_EIA_res_rest.where(
        (df_EIA_res_rest.BEDSIZE >= 100) &
        (df_EIA_res_rest.City == ct) &
        (df_EIA_res_rest.PANEL == 0)
    ).groupBy("scen_id", "Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
        .withColumnRenamed("sum(加罗宁)", "加罗宁_p0_bed100") \
        .withColumnRenamed("sum(凯纷)", "凯纷_p0_bed100") \
        .withColumnRenamed("sum(诺扬)", "诺扬_p0_bed100") \
        .withColumnRenamed("sum(其它)", "其它_p0_bed100") \
        .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p0_bed100").fillna(0.0)

    df_rest_seg_p1_bed100 = df_EIA_res_rest.where(
        (df_EIA_res_rest.BEDSIZE >= 100) &
        (df_EIA_res_rest.City == ct) &
        (df_EIA_res_rest.PANEL == 1)
    ).groupBy("scen_id", "Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
        .withColumnRenamed("sum(加罗宁)", "加罗宁_p1_bed100") \
        .withColumnRenamed("sum(凯纷)", "凯纷_p1_bed100") \
        .withColumnRenamed("sum(诺扬)", "诺扬_p1_bed100") \
        .withColumnRenamed("sum(其它)", "其它_p1_bed100") \
        .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p1_bed100").fillna(0.0)

    df_rest_seg_p1 = df_EIA_res_rest.where(
        (df_EIA_res_rest.PANEL == 1)
    ).groupBy("scen_id", "Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
        .withColumnRenamed("sum(加罗宁)", "加罗宁_p1_other") \
        .withColumnRenamed("sum(凯纷)", "凯纷_p1_other") \
        .withColumnRenamed("sum(诺扬)", "诺扬_p1_other") \
        .withColumnRenamed("sum(其它)", "其它_p1_other") \
        .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p1_other").fillna(0.0)

    df_rest_seg = df_rest_seg.join(df_rest_seg_p0_bed100, on=["scen_id", "Date"], how="left")
    df_rest_seg = df_rest_seg.join(df_rest_seg_p1_bed100, on=["scen_id", "Date"], how="left")
    df_rest_seg = df_rest_seg.join(df_rest_seg_p1, on=["scen_id", "Date"], how="left").fillna(0.0)

    df_rest_seg_p0_bed100_scen_count = df_rest_seg_p0_bed100.withColumn("count", func.lit(1))
    df_rest_seg_p0_bed100_scen_count = df_rest_seg_p0_bed100_scen_count.groupBy("scen_id")\
        .sum("count").withColumnRenamed("sum(count)", "count_p0_bed100")
    df_rest_seg = df_rest_seg.join(df_rest_seg_p0_bed100_scen_count, on="scen_id", how="left")
    df_rest_seg = df_rest_seg.withColumn("w",
                                         func.when(df_rest_seg.count_p0_bed100 == 0, func.lit(0))
                                         .otherwise(df_rest_seg.Est_DrugIncome_RMB_p0_bed100 / df_rest_seg.Est_DrugIncome_RMB_p1_other))

    for iprd in prd_input:
        df_rest_seg = df_rest_seg \
            .withColumn(iprd + "_fd",
                        df_rest_seg[iprd] * df_rest_seg.w + df_rest_seg[iprd + "_p1_bed100"])

    df_rest_seg = df_rest_seg \
        .withColumn("oth_fd",
                    df_rest_seg["其它"] * df_rest_seg.w + df_rest_seg["其它_p1_bed100"])


    df_rest_poi_oth = df_rest_seg.groupBy("scen_id").sum("加罗宁_fd", "凯纷_fd", "诺扬_fd", "oth_fd") \
        .withColumnRenamed("sum(加罗宁_fd)", "加罗宁_rest_poi") \
        .withColumnRenamed("sum(凯纷_fd)", "凯纷_rest_poi") \
        .withColumnRenamed("sum(诺扬_fd)", "诺扬_rest_poi") \
        .withColumnRenamed("sum(oth_fd)", "oth_rest_oth")

    df_result = df_EIA_res_cur_poi_ot.join(df_rest_poi_oth, on="scen_id")
    # df_result.show()

    other_seg_poi_sum = sum(other_seg_poi.values())
    df_result = df_result.withColumn("mkt_vol",
                                     df_result["加罗宁_rest_poi"] + df_result["凯纷_rest_poi"] +
                                     df_result["诺扬_rest_poi"] + df_result["oth_rest_oth"] +
                                     df_result["加罗宁_poi_ot"] + df_result["凯纷_poi_ot"] +
                                     df_result["诺扬_poi_ot"] + df_result["oth_ot"] + func.lit(other_seg_oth) +
                                     func.lit(other_seg_poi_sum))

    df_result = df_result.withColumn("加罗宁",
                                     df_result["加罗宁_rest_poi"] +
                                     df_result["加罗宁_poi_ot"] + func.lit(other_seg_poi["加罗宁"]))
    df_result = df_result.withColumn("凯纷",
                                     df_result["凯纷_rest_poi"] +
                                     df_result["凯纷_poi_ot"] + func.lit(other_seg_poi["凯纷"]))
    df_result = df_result.withColumn("诺扬",
                                     df_result["诺扬_rest_poi"] +
                                     df_result["诺扬_poi_ot"] + func.lit(other_seg_poi["诺扬"]))

    df_result = df_result.select("scen_id", "加罗宁", "凯纷", "诺扬", "mkt_vol")
    print df_result.count()
    df_result.show()
    df_result.where(df_result.scen_id == 0).show()

    return df_EIA_res_cur
