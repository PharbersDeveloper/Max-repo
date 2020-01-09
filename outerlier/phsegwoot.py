# coding=utf-8
from pyspark.sql.types import *
from pyspark.sql import functions as func
import pandas as pd
import numpy as np


def max_outlier_seg_wo_ot_old(spark, df_EIA_res_iter, ct, seg_wo_ot):
    prd_input = ["加罗宁", "凯纷", "诺扬"]
    seg_wo_ot_poi = {}
    seg_wo_ot_oth = {}
    other_seg_poi = {}
    for z in range(len(seg_wo_ot)):
        df_other_seg = df_EIA_res_iter.where(df_EIA_res_iter.Seg == seg_wo_ot[z])
        # df_other_seg.show()
        # df_other_seg_panel = df_EIA_res_iter.where(
        #     (df_EIA_res_iter.Seg == seg_wo_ot[z]) &
        #     (df_EIA_res_iter.PANEL == 1)
        # )

        df_oth_seg = df_other_seg.groupBy("Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            .withColumnRenamed("sum(加罗宁)", "加罗宁") \
            .withColumnRenamed("sum(凯纷)", "凯纷") \
            .withColumnRenamed("sum(诺扬)", "诺扬") \
            .withColumnRenamed("sum(其它)", "其它") \
            .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB")

        oth_seg_p0_bed100 = df_other_seg.where(
            (df_other_seg.BEDSIZE >= 100) &
            (df_other_seg.City == ct) &
            (df_other_seg.PANEL == 0)
        ).groupBy("Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            .withColumnRenamed("sum(加罗宁)", "加罗宁") \
            .withColumnRenamed("sum(凯纷)", "凯纷") \
            .withColumnRenamed("sum(诺扬)", "诺扬") \
            .withColumnRenamed("sum(其它)", "其它") \
            .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB").toPandas().set_index("Date")

        oth_seg_p1_bed100 = df_other_seg.where(
            (df_other_seg.BEDSIZE >= 100) &
            (df_other_seg.City == ct) &
            (df_other_seg.PANEL == 1)
        ).groupBy("Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            .withColumnRenamed("sum(加罗宁)", "加罗宁") \
            .withColumnRenamed("sum(凯纷)", "凯纷") \
            .withColumnRenamed("sum(诺扬)", "诺扬") \
            .withColumnRenamed("sum(其它)", "其它") \
            .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB").toPandas().set_index("Date")

        oth_seg_p1 = df_other_seg.where(
            (df_other_seg.PANEL == 1)
        ).groupBy("Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            .withColumnRenamed("sum(加罗宁)", "加罗宁") \
            .withColumnRenamed("sum(凯纷)", "凯纷") \
            .withColumnRenamed("sum(诺扬)", "诺扬") \
            .withColumnRenamed("sum(其它)", "其它") \
            .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB").toPandas().set_index("Date")

        oth_seg = df_oth_seg.toPandas().set_index("Date")

        if len(oth_seg_p0_bed100["Est_DrugIncome_RMB"]) == 0:
            oth_seg["w"] = oth_seg["Est_DrugIncome_RMB"] * 0
        else:
            oth_seg["w"] = np.divide(oth_seg_p0_bed100["Est_DrugIncome_RMB"], oth_seg_p1["Est_DrugIncome_RMB"])

        if len(oth_seg_p1_bed100["Est_DrugIncome_RMB"]) == 0:
            oth_seg_p1_bed100[prd_input + ["其它"]] = oth_seg[prd_input + ["其它"]].fillna(0) * 0

        oth_seg["oth_fd"] = oth_seg["其它"].fillna(0) * oth_seg["w"] + oth_seg_p1_bed100["其它"].fillna(0)

        for iprd in prd_input:
            oth_seg[iprd + "_fd"] = oth_seg[iprd].fillna(0) * oth_seg["w"] + oth_seg_p1_bed100[iprd].fillna(0)
            if seg_wo_ot_poi.get(iprd):
                seg_wo_ot_poi[iprd].update({z: oth_seg[iprd + "_fd"].sum()})
            else:
                seg_wo_ot_poi[iprd] = {z: oth_seg[iprd + "_fd"].sum()}

        seg_wo_ot_oth[z] = oth_seg["oth_fd"].sum()

    for iprd in prd_input:
        other_seg_poi[iprd] = sum(seg_wo_ot_poi[iprd].values())

    other_seg_oth = sum(seg_wo_ot_oth.values())

    return [other_seg_oth, other_seg_poi]


def max_outlier_seg_wo_ot_spark(spark, df_EIA_res_iter, ct, seg_wo_ot, ):
    prd_input = ["加罗宁", "凯纷", "诺扬"]
    schema = StructType([
        StructField("加罗宁_fd", DoubleType(), True),
        StructField("凯纷_fd", DoubleType(), True),
        StructField("诺扬_fd", DoubleType(), True),
        StructField("oth_fd", DoubleType(), True)])
    df_result = spark.createDataFrame([(0.0, 0.0, 0.0, 0.0)], schema)
    for z in seg_wo_ot:
        df_other_seg = df_EIA_res_iter.where(df_EIA_res_iter.Seg == z)

        # df_other_seg.show()
        # df_other_seg_panel = df_EIA_res_iter.where(
        #     (df_EIA_res_iter.Seg == z) &
        #     (df_EIA_res_iter.PANEL == 1)
        # )

        df_oth_seg = df_other_seg.groupBy("Date", "Seg").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            .withColumnRenamed("sum(加罗宁)", "加罗宁") \
            .withColumnRenamed("sum(凯纷)", "凯纷") \
            .withColumnRenamed("sum(诺扬)", "诺扬") \
            .withColumnRenamed("sum(其它)", "其它") \
            .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB")

        df_oth_seg_p0_bed100 = df_other_seg.where(
            (df_other_seg.BEDSIZE >= 100) &
            (df_other_seg.City == ct) &
            (df_other_seg.PANEL == 0)
        ).groupBy("Date", "Seg").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            .withColumnRenamed("sum(加罗宁)", "加罗宁_p0_bed100") \
            .withColumnRenamed("sum(凯纷)", "凯纷_p0_bed100") \
            .withColumnRenamed("sum(诺扬)", "诺扬_p0_bed100") \
            .withColumnRenamed("sum(其它)", "其它_p0_bed100") \
            .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p0_bed100")

        df_oth_seg_p1_bed100 = df_other_seg.where(
            (df_other_seg.BEDSIZE >= 100) &
            (df_other_seg.City == ct) &
            (df_other_seg.PANEL == 1)
        ).groupBy("Date", "Seg").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            .withColumnRenamed("sum(加罗宁)", "加罗宁_p1_bed100") \
            .withColumnRenamed("sum(凯纷)", "凯纷_p1_bed100") \
            .withColumnRenamed("sum(诺扬)", "诺扬_p1_bed100") \
            .withColumnRenamed("sum(其它)", "其它_p1_bed100") \
            .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p1_bed100")

        df_oth_seg_p1 = df_other_seg.where(
            (df_other_seg.PANEL == 1)
        ).groupBy("Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            .withColumnRenamed("sum(加罗宁)", "加罗宁_p1_other") \
            .withColumnRenamed("sum(凯纷)", "凯纷_p1_other") \
            .withColumnRenamed("sum(诺扬)", "诺扬_p1_other") \
            .withColumnRenamed("sum(其它)", "其它_p1_other") \
            .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p1_other")

        df_oth_seg = df_oth_seg.join(df_oth_seg_p0_bed100, on="Date", how="left")
        df_oth_seg = df_oth_seg.join(df_oth_seg_p1_bed100, on="Date", how="left")
        df_oth_seg = df_oth_seg.join(df_oth_seg_p1, on="Date", how="left")

        if df_oth_seg_p0_bed100.count() == 0:
            df_oth_seg = df_oth_seg.withColumn("w", func.lit(0))
        else:
            df_oth_seg = df_oth_seg \
                .withColumn("w", df_oth_seg.Est_DrugIncome_RMB_p0_bed100 / df_oth_seg.Est_DrugIncome_RMB_p1_other)

        if df_oth_seg_p1_bed100.count() == 0:
            df_oth_seg = df_oth_seg.withColumn("加罗宁_p1_bed100", func.lit(0)) \
                .withColumn("凯纷_p1_bed100", func.lit(0)) \
                .withColumn("诺扬_p1_bed100", func.lit(0)) \
                .withColumn("其它_p1_bed100", func.lit(0))

        df_oth_seg = df_oth_seg.withColumn("oth_fd", df_oth_seg["其它"] * df_oth_seg.w + df_oth_seg["其它_p1_bed100"])

        for iprd in prd_input:
            df_oth_seg = df_oth_seg \
                .withColumn(iprd + "_fd", df_oth_seg[iprd] * df_oth_seg.w + df_oth_seg[iprd + "_p1_bed100"])

        # df_result = df_result.union(df_oth_seg.groupBy().sum("加罗宁_fd", "凯纷_fd", "诺扬_fd", "oth_fd"))
        df_result = df_result.union(df_oth_seg.select("加罗宁_fd", "凯纷_fd", "诺扬_fd", "oth_fd"))

    sum_result = df_result.groupBy().sum("加罗宁_fd", "凯纷_fd", "诺扬_fd", "oth_fd").toPandas()
    print sum_result
    other_seg_poi = {}
    for iprd in prd_input:
        other_seg_poi[iprd] = sum_result.at[0, "sum(" + iprd + "_fd)"]

    other_seg_oth = sum_result.at[0, "sum(oth_fd)"]

    return [other_seg_oth, other_seg_poi]
