# coding=utf-8
from pyspark.sql.types import *
from pyspark.sql import functions as func
import pandas as pd
import numpy as np
from phOutlierParameters import prd_input
from phRename import udf_rename
from phNewMultiColumns import udf_new_columns
from phSetSchema import udf_add_struct


def max_outlier_seg_wo_ot_old(spark, df_EIA_res_iter, ct, seg_wo_ot):
    # prd_input = ["加罗宁", "凯纷", "诺扬"]
    seg_wo_ot_poi = {}
    seg_wo_ot_oth = {}
    other_seg_poi = {}
    for z in range(len(seg_wo_ot)):
        df_oth_seg = df_EIA_res_iter.where(df_EIA_res_iter.Seg == seg_wo_ot[z])
        # df_oth_seg.show()
        # df_oth_seg_panel = df_EIA_res_iter.where(
        #     (df_EIA_res_iter.Seg == seg_wo_ot[z]) &
        #     (df_EIA_res_iter.PANEL == 1)
        # )

        # df_oth_seg = df_oth_seg.groupBy("Date").sum("加罗宁", "凯纷", "诺扬", "other", "Est_DrugIncome_RMB") \
        #     .withColumnRenamed("sum(加罗宁)", "加罗宁") \
        #     .withColumnRenamed("sum(凯纷)", "凯纷") \
        #     .withColumnRenamed("sum(诺扬)", "诺扬") \
        #     .withColumnRenamed("sum(other)", "other") \
        #     .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB")
        df_oth_seg = df_other_seg.groupBy("Date").sum(*prd_input+["other", "Est_DrugIncome_RMB"])
        df_oth_seg = udf_rename(df_oth_seg, prd_input+["other", "Est_DrugIncome_RMB"])

        oth_seg_p0_bed100 = df_other_seg.where(
            (df_other_seg.BEDSIZE >= 100) &
            (df_other_seg.City == ct) &
            (df_other_seg.PANEL == 0)
        ).groupBy("Date").sum(*prd_input+["other", "Est_DrugIncome_RMB"])

        oth_seg_p0_bed100 = udf_rename(oth_seg_p0_bed100, prd_input+["other", "Est_DrugIncome_RMB"]).\
            toPandas().set_index("Date")

        oth_seg_p1_bed100 = df_other_seg.where(
            (df_other_seg.BEDSIZE >= 100) &
            (df_other_seg.City == ct) &
            (df_other_seg.PANEL == 1)
        ).groupBy("Date").sum(*prd_input+["other", "Est_DrugIncome_RMB"])

        oth_seg_p1_bed100 = udf_rename(oth_seg_p1_bed100, prd_input+["other", "Est_DrugIncome_RMB"]).\
            toPandas().set_index("Date")

        oth_seg_p1 = df_other_seg.where(
            (df_other_seg.PANEL == 1)
        ).groupBy("Date").sum(*prd_input+["other", "Est_DrugIncome_RMB"])

        oth_seg_p1 = udf_rename(oth_seg_p1, prd_input+["other", "Est_DrugIncome_RMB"]).toPandas().set_index("Date")

        oth_seg = df_oth_seg.toPandas().set_index("Date")

        if len(oth_seg_p0_bed100["Est_DrugIncome_RMB"]) == 0:
            oth_seg["w"] = oth_seg["Est_DrugIncome_RMB"] * 0
        else:
            oth_seg["w"] = np.divide(oth_seg_p0_bed100["Est_DrugIncome_RMB"], oth_seg_p1["Est_DrugIncome_RMB"])

        if len(oth_seg_p1_bed100["Est_DrugIncome_RMB"]) == 0:
            oth_seg_p1_bed100[prd_input + ["other"]] = oth_seg[prd_input + ["other"]].fillna(0) * 0

        oth_seg["other_fd"] = oth_seg["other"].fillna(0) * oth_seg["w"] + oth_seg_p1_bed100["other"].fillna(0)

        for iprd in prd_input:
            oth_seg[iprd + "_fd"] = oth_seg[iprd].fillna(0) * oth_seg["w"] + oth_seg_p1_bed100[iprd].fillna(0)
            if seg_wo_ot_poi.get(iprd):
                seg_wo_ot_poi[iprd].update({z: oth_seg[iprd + "_fd"].sum()})
            else:
                seg_wo_ot_poi[iprd] = {z: oth_seg[iprd + "_fd"].sum()}

        seg_wo_ot_oth[z] = oth_seg["other_fd"].sum()

    for iprd in prd_input:
        other_seg_poi[iprd] = sum(seg_wo_ot_poi[iprd].values())

    other_seg_oth = sum(seg_wo_ot_oth.values())

    return [other_seg_oth, other_seg_poi]


def max_outlier_seg_wo_ot_spark(spark, df_EIA_res_iter, ct, seg_wo_ot, ):
    # prd_input = ["加罗宁", "凯纷", "诺扬"]
    # schema = StructType([
    #     StructField("加罗宁_fd", DoubleType(), True),
    #     StructField("凯纷_fd", DoubleType(), True),
    #     StructField("诺扬_fd", DoubleType(), True),
    #     StructField("other_fd", DoubleType(), True)])
    schema = udf_add_struct([p+"_fd" for p in prd_input] + ["other_fd"])
    df_result = spark.createDataFrame([], schema)
    for z in seg_wo_ot:
        df_other_seg = df_EIA_res_iter.where(df_EIA_res_iter.Seg == z)

        # df_oth_seg.show()
        # df_oth_seg_panel = df_EIA_res_iter.where(
        #     (df_EIA_res_iter.Seg == z) &
        #     (df_EIA_res_iter.PANEL == 1)
        # )

        df_oth_seg = df_other_seg.groupBy("Date").sum(*prd_input+["other", "Est_DrugIncome_RMB"])

        df_oth_seg = udf_rename(df_oth_seg, prd_input+["other", "Est_DrugIncome_RMB"])

        df_oth_seg_p0_bed100 = df_other_seg.where(
            (df_other_seg.BEDSIZE >= 100) &
            (df_other_seg.City == ct) &
            (df_other_seg.PANEL == 0)
        ).groupBy("Date").sum(*prd_input+["other", "Est_DrugIncome_RMB"])

        df_oth_seg_p0_bed100 = udf_rename(df_oth_seg_p0_bed100, \
                                          prd_input+["other", "Est_DrugIncome_RMB"], "_p0_bed100")

        df_oth_seg_p1_bed100 = df_other_seg.where(
            (df_other_seg.BEDSIZE >= 100) &
            (df_other_seg.City == ct) &
            (df_other_seg.PANEL == 1)
        ).groupBy("Date").sum(*prd_input+["other", "Est_DrugIncome_RMB"])

        df_oth_seg_p1_bed100 = udf_rename(df_oth_seg_p1_bed100, \
                                          prd_input+["other", "Est_DrugIncome_RMB"], "_p1_bed100")

        df_oth_seg_p1 = df_other_seg.where(
            (df_other_seg.PANEL == 1)
        ).groupBy("Date").sum(*prd_input+["other", "Est_DrugIncome_RMB"])

        df_oth_seg_p1 = udf_rename(df_oth_seg_p1,\
                                   prd_input+["other", "Est_DrugIncome_RMB"], "_p1_other")


        df_oth_seg = df_oth_seg.join(df_oth_seg_p0_bed100, on="Date", how="left")
        df_oth_seg = df_oth_seg.join(df_oth_seg_p1_bed100, on="Date", how="left")
        df_oth_seg = df_oth_seg.join(df_oth_seg_p1, on="Date", how="left").fillna(0)

        if df_oth_seg_p0_bed100.count() == 0:
            df_oth_seg = df_oth_seg.withColumn("w", func.lit(0))
        else:
            df_oth_seg = df_oth_seg \
                .withColumn("w", df_oth_seg.Est_DrugIncome_RMB_p0_bed100 / df_oth_seg.Est_DrugIncome_RMB_p1_other)

        if df_oth_seg_p1_bed100.count() == 0:
            df_oth_seg = udf_new_columns(df_oth_seg, prd_input, "_p1_bed100")
            # df_oth_seg = df_oth_seg.withColumn("加罗宁_p1_bed100", func.lit(0)) \
            #     .withColumn("凯纷_p1_bed100", func.lit(0)) \
            #     .withColumn("诺扬_p1_bed100", func.lit(0)) \
            #     .withColumn("other_p1_bed100", func.lit(0))

        df_oth_seg = df_oth_seg.withColumn("other_fd", df_oth_seg["other"] * df_oth_seg.w + df_oth_seg["other_p1_bed100"])

        for iprd in prd_input:
            df_oth_seg = df_oth_seg \
                .withColumn(iprd + "_fd", df_oth_seg[iprd] * df_oth_seg.w + df_oth_seg[iprd + "_p1_bed100"])

        # df_result = df_result.union(df_oth_seg.groupBy().sum("加罗宁_fd", "凯纷_fd", "诺扬_fd", "other_fd"))
        df_result = df_result.union(df_oth_seg.select(*[p+"_fd" for p in prd_input]+["other_fd"]))
        # [p+"_fd" for p in prd_input], "other_fd"
    sum_result = df_result.groupBy().sum(*[p+"_fd" for p in prd_input]+["other_fd"]).toPandas()
    #sum_result = udf_rename(sum_result, [p+"_fd" for p in prd_input]+["other_fd"]).toPandas()
    print sum_result.columns
    print sum_result
    #chk = sum_result.at[0, 1]
    other_seg_oth = sum_result.at[0, "sum(other_fd)"]
    other_seg_poi = {}
    for iprd in prd_input:

        print "sum(" + iprd + "_fd)"

        #other_seg_poi[iprd] = sum_result.at[0, iprd+"_fd"]
        other_seg_poi[iprd] = sum_result.at[0, ("sum(" + iprd + "_fd)").encode("utf-8")]

    #other_seg_oth = sum_result.at[0, "other_fd"]
    #other_seg_oth = sum_result.at[0, "sum(other_fd)"]

    return [other_seg_oth, other_seg_poi]
