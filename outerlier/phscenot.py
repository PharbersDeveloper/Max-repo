# coding=utf-8
from pyspark.sql.types import *
from pyspark.sql import functions as func
from phOutlierParameters import prd_input, sql_content
from phRename import udf_rename
from phNewMultiColumns import udf_new_columns
from phColumnsAdding import sum_columns

def max_outlier_seg_scen_ot_spark(spark, df_EIA_res_cur,
                                  df_panel, ct, scen,
                                  ot_seg, other_seg_poi, other_seg_oth):
    #prd_input = ["加罗宁", "凯纷", "诺扬"]

    schema = StructType([
        StructField("poi", StringType(), True),
        StructField("scen_id", IntegerType(), True),
        StructField("share", DoubleType(), True),
        StructField("num_ot", DoubleType(), True),
        StructField("vol_ot", DoubleType(), True),
        StructField("poi_vol", DoubleType(), True),
        StructField("mkt_vol", DoubleType(), True),
        StructField("scen", StringType(), True),
        StructField("city", StringType(), True)
    ])

    for i in range(2):
    # for i in range(len(scen[ot_seg])):
        print u"current index %d" % i
        panel_sc = df_panel.where(df_panel.HOSP_ID.isin(scen[ot_seg][i])).toPandas()
        condi = panel_sc["HOSP_ID"].to_numpy().tolist()
        # print condi

        num_ot = len(scen[ot_seg][i])
        vol_ot = df_EIA_res_cur \
            .where(df_EIA_res_cur.HOSP_ID.isin(condi)) \
            .groupBy().sum("Est_DrugIncome_RMB") \
            .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB") \
            .na.fill(0).toPandas()["Est_DrugIncome_RMB"].to_numpy()[0]

        df_EIA_res_cur_ct_b100 = df_EIA_res_cur.where(
            (df_EIA_res_cur.BEDSIZE >= 100) & (df_EIA_res_cur.City == ct))

        # df_poi_ot = df_EIA_res_cur_ct_b100 \
        #     .where(df_EIA_res_cur_ct_b100.HOSP_ID.isin(condi)).groupBy() \
        #     .sum("加罗宁", "凯纷", "诺扬") \
        #     .withColumnRenamed("sum(加罗宁)", "加罗宁") \
        #     .withColumnRenamed("sum(凯纷)", "凯纷") \
        #     .withColumnRenamed("sum(诺扬)", "诺扬").fillna(0.0)
        df_poi_ot = df_EIA_res_cur_ct_b100 \
            .where(df_EIA_res_cur_ct_b100.HOSP_ID.isin(condi)).groupBy() \
            .sum(*prd_input)
        df_poi_ot = udf_rename(df_poi_ot, prd_input).fillna(0.0)

        df_poi_ot.show()

        df_oth_ot = df_EIA_res_cur_ct_b100 \
            .where(df_EIA_res_cur_ct_b100.HOSP_ID.isin(condi)).groupBy() \
            .sum("other") \
            .withColumnRenamed("sum(other)", "other").fillna(0.0)
        df_oth_ot.show()

        df_EIA_res_rest = df_EIA_res_cur.where(~df_EIA_res_cur.HOSP_ID.isin(condi))
        df_EIA_res_rest_panel = df_EIA_res_rest.where(df_EIA_res_rest.PANEL == 1)

        # df_rest_seg = df_EIA_res_rest.groupBy("Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
        #     .withColumnRenamed("sum(加罗宁)", "加罗宁") \
        #     .withColumnRenamed("sum(凯纷)", "凯纷") \
        #     .withColumnRenamed("sum(诺扬)", "诺扬") \
        #     .withColumnRenamed("sum(其它)", "其它") \
        #     .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB").fillna(0.0)

        df_rest_seg = df_EIA_res_rest.groupBy("Date").sum(*[p for p in prd_input]+["other","Est_DrugIncome_RMB"])
        df_rest_seg = udf_rename(df_rest_seg, [p for p in prd_input]+["other","Est_DrugIncome_RMB"])
        df_rest_seg = df_rest_seg.fillna(0.0)

        df_rest_seg_p0_bed100 = df_EIA_res_rest.where(
            (df_EIA_res_rest.BEDSIZE >= 100) &
            (df_EIA_res_rest.City == ct) &
            (df_EIA_res_rest.PANEL == 0)
        )#.groupBy("Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            # .withColumnRenamed("sum(加罗宁)", "加罗宁_p0_bed100") \
            # .withColumnRenamed("sum(凯纷)", "凯纷_p0_bed100") \
            # .withColumnRenamed("sum(诺扬)", "诺扬_p0_bed100") \
            # .withColumnRenamed("sum(其它)", "其它_p0_bed100") \
            # .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p0_bed100").fillna(0.0)
        df_rest_seg_p0_bed100 = df_rest_seg_p0_bed100.groupBy("Date").sum(*[p for p in prd_input]+["other","Est_DrugIncome_RMB"])
        df_rest_seg_p0_bed100 = udf_rename(df_rest_seg_p0_bed100, [p for p in prd_input]+["other","Est_DrugIncome_RMB"],"_p0_bed100")
        df_rest_seg_p0_bed100 = df_rest_seg_p0_bed100.fillna(0.0)

        df_rest_seg_p1_bed100 = df_EIA_res_rest.where(
            (df_EIA_res_rest.BEDSIZE >= 100) &
            (df_EIA_res_rest.City == ct) &
            (df_EIA_res_rest.PANEL == 1)
        )#.groupBy("Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            # .withColumnRenamed("sum(加罗宁)", "加罗宁_p1_bed100") \
            # .withColumnRenamed("sum(凯纷)", "凯纷_p1_bed100") \
            # .withColumnRenamed("sum(诺扬)", "诺扬_p1_bed100") \
            # .withColumnRenamed("sum(其它)", "其它_p1_bed100") \
            # .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p1_bed100").fillna(0.0)
        df_rest_seg_p1_bed100 = df_rest_seg_p1_bed100.groupBy("Date").sum(*[p for p in prd_input]+["other","Est_DrugIncome_RMB"])
        df_rest_seg_p1_bed100 = udf_rename(df_rest_seg_p1_bed100, [p for p in prd_input]+["other","Est_DrugIncome_RMB"],"_p1_bed100")
        df_rest_seg_p1_bed100 = df_rest_seg_p1_bed100.fillna(0.0)

        df_rest_seg_p1 = df_EIA_res_rest.where(
            (df_EIA_res_rest.PANEL == 1)
        )#.groupBy("Date").sum("加罗宁", "凯纷", "诺扬", "其它", "Est_DrugIncome_RMB") \
            # .withColumnRenamed("sum(加罗宁)", "加罗宁_p1_other") \
            # .withColumnRenamed("sum(凯纷)", "凯纷_p1_other") \
            # .withColumnRenamed("sum(诺扬)", "诺扬_p1_other") \
            # .withColumnRenamed("sum(其它)", "其它_p1_other") \
            # .withColumnRenamed("sum(Est_DrugIncome_RMB)", "Est_DrugIncome_RMB_p1_other").fillna(0.0)
        df_rest_seg_p1 = df_rest_seg_p1.groupBy("Date").sum(*[p for p in prd_input]+["other","Est_DrugIncome_RMB"])
        df_rest_seg_p1 = udf_rename(df_rest_seg_p1, [p for p in prd_input]+["other","Est_DrugIncome_RMB"],"_p1_other")
        df_rest_seg_p1 = df_rest_seg_p1.fillna(0.0)

        df_rest_seg = df_rest_seg.join(df_rest_seg_p0_bed100, on="Date", how="left")
        df_rest_seg = df_rest_seg.join(df_rest_seg_p1_bed100, on="Date", how="left")
        df_rest_seg = df_rest_seg.join(df_rest_seg_p1, on="Date", how="left").fillna(0.0)

        if df_rest_seg_p0_bed100.count() == 0:
            df_rest_seg = df_rest_seg.withColumn("w", func.lit(0))
        else:
            df_rest_seg = df_rest_seg \
                .withColumn("w",
                            df_rest_seg.Est_DrugIncome_RMB_p0_bed100 / df_rest_seg.Est_DrugIncome_RMB_p1_other)

        for iprd in prd_input:
            df_rest_seg = df_rest_seg \
                .withColumn(iprd + "_fd",
                            df_rest_seg[iprd] * df_rest_seg.w + df_rest_seg[iprd + "_p1_bed100"])

        df_rest_seg = df_rest_seg \
            .withColumn("other_fd",
                        df_rest_seg["other"] * df_rest_seg.w + df_rest_seg["other_p1_bed100"])

        # df_rest_poi = df_rest_seg.groupBy().sum("加罗宁_fd", "凯纷_fd", "诺扬_fd") \
        #     .withColumnRenamed("sum(加罗宁_fd)", "加罗宁_fd") \
        #     .withColumnRenamed("sum(凯纷_fd)", "凯纷_fd") \
        #     .withColumnRenamed("sum(诺扬_fd)", "诺扬_fd")

        df_rest_poi = df_rest_seg.groupBy("Date").sum(*[p+"_fd" for p in prd_input])
        df_rest_poi = udf_rename(df_rest_poi, [p+"_fd" for p in prd_input])



        df_rest_oth = df_rest_seg.groupBy().sum("other_fd") \
            .withColumnRenamed("sum(other_fd)", "other_fd")

        # df_rest_poi = df_rest_poi \
        #     .withColumnRenamed("加罗宁_fd", "加罗宁_rest_poi") \
        #     .withColumnRenamed("凯纷_fd", "凯纷_rest_poi") \
        #     .withColumnRenamed("诺扬_fd", "诺扬_rest_poi") \
        #     .withColumn("k", func.lit(1))
        df_rest_poi = udf_rename(df_rest_poi, [p+"_fd" for p in prd_input],"_rest_poi")
        df_rest_poi = udf_new_columns(df_rest_poi, ["k"], "", 1)

        df_rest_oth = df_rest_oth \
            .withColumnRenamed("other_fd", "other_fd_rest_poi") \
            .withColumn("k", func.lit(1))

        # df_poi_ot = df_poi_ot \
        #     .withColumnRenamed("加罗宁", "加罗宁_poi_ot") \
        #     .withColumnRenamed("凯纷", "凯纷_poi_ot") \
        #     .withColumnRenamed("诺扬", "诺扬_poi_ot") \
        #     .withColumn("k", func.lit(1))
        df_poi_ot = udf_rename(df_poi_ot, [p for p in prd_input],"_poi_ot")
        df_poi_ot = udf_new_columns(df_poi_ot, ["k"], "", 1)

        df_oth_ot = df_oth_ot \
            .withColumnRenamed("other", "other_ot") \
            .withColumn("k", func.lit(1))

        df_result = df_rest_poi.join(df_rest_oth, on="k").join(df_poi_ot, on="k").join(df_oth_ot, on="k")
        df_result = df_result.fillna(0.0)

        other_seg_poi_sum = sum(other_seg_poi.values())
        # df_result = df_result.withColumn("mkt_vol",
        #                                  df_result["加罗宁_rest_poi"] + df_result["凯纷_rest_poi"] +
        #                                  df_result["诺扬_rest_poi"] + df_result["oth_rest_oth"] +
        #                                  df_result["加罗宁_poi_ot"] + df_result["凯纷_poi_ot"] +
        #                                  df_result["诺扬_poi_ot"] + df_result["oth_ot"] + func.lit(other_seg_oth) +
        #                                  func.lit(other_seg_poi_sum))
        df_result = sum_columns(df_result, [p+"_rest_poi" for p in prd_input+["other"]]+ \
                                [p+"_poi_ot" for p in prd_input+["other"]], "mkt_vol")
        df_result = df_result.withColumn("mkt_vol",
                                         df_result["mkt_vol"]+func.lit(other_seg_oth) +
                                         func.lit(other_seg_poi_sum))

        # df_result = df_result.withColumn("加罗宁",
        #                                  df_result["加罗宁_rest_poi"] +
        #                                  df_result["加罗宁_poi_ot"] + func.lit(other_seg_poi["加罗宁"]))
        # df_result = df_result.withColumn("凯纷",
        #                                  df_result["凯纷_rest_poi"] +
        #                                  df_result["凯纷_poi_ot"] + func.lit(other_seg_poi["凯纷"]))
        # df_result = df_result.withColumn("诺扬",
        #                                  df_result["诺扬_rest_poi"] +
        #                                  df_result["诺扬_poi_ot"] + func.lit(other_seg_poi["诺扬"]))
        for p in prd_input:
            df_result = sum_columns(df_result, [p+"_rest_poi",p+"_poi_ot"], p)
            df_result = df_result.withColumn(p,
                                             df_result[p]+func.lit(other_seg_poi[p]))

        df_result = df_result.select(*prd_input+ ["mkt_vol"])
        df_result.show()

        df_result.createOrReplaceTempView('v_pivot')
        # sql_content = '''select `mkt_vol`,
        #              stack(3, '加罗宁', `加罗宁`, '凯纷', `凯纷`, '诺扬', `诺扬`) as (`poi`, `poi_vol` )
        #              from  v_pivot
        #           '''

        df_result = spark.sql(sql_content)
        df_result = df_result.withColumn("scen_id", func.lit(i)) \
            .withColumn("share", df_result.poi_vol / df_result.mkt_vol) \
            .withColumn("num_ot", func.lit(num_ot)) \
            .withColumn("vol_ot", func.lit(vol_ot)) \
            .withColumn("scen", func.lit(",".join(scen[ot_seg][i]))) \
            .withColumn("city", func.lit(ct)) \
            .select("poi", "scen_id", "share", "num_ot", "vol_ot", "poi_vol", "mkt_vol", "scen", "city")

        df_result.show()
        df_result.write.format("parquet") \
            .mode("append").save(u"hdfs://192.168.100.137/user/alfredyang/outlier/result1")
        # df_scen_result = df_scen_result.union(df_result)

    df_scen_result = spark.read.parquet(u"hdfs://192.168.100.137/user/alfredyang/outlier/result1")
    return df_scen_result
