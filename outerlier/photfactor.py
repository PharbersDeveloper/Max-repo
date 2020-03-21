# coding=utf-8
import time

from pyspark.sql.types import *
import numpy as np
from cvxpy import *
from phOutlierParameters import prd_input,tmp_df_factor_result_path


def max_outlier_factor_inner_loop_old(spark, rlt_scs, df_rlt, fst_prd, bias, index):
    for isc in rlt_scs:
        rltsc = df_rlt[df_rlt["scen_id"]==isc].reset_index(drop=False).fillna(0)
        #df_rlt_sc.show()
        # 一个线下算法库，没有替代品的情况下线下计算
        # rltsc = df_rlt_sc.toPandas()
        print (len(rltsc))
        f = Variable()
        poi_ratio = {}
        mkt_ratio = {}

        for iprd in range(len(rltsc.index)):
            poi_ratio[iprd] = np.divide(
                (rltsc["poi_vol"][iprd] - rltsc["sales_pnl"][iprd]) * f + rltsc["sales_pnl"][iprd],
                rltsc["ims_poi_vol"][iprd]) - 1
            mkt_ratio[iprd] = np.divide(
                (rltsc["mkt_vol"][iprd] - rltsc["sales_pnl_mkt"][iprd]) * f + rltsc["sales_pnl_mkt"][iprd],
                rltsc["ims_mkt_vol"][iprd]) - 1

        par = []
        for s in range(len(rltsc.index)):
            if rltsc["poi"][s] in prd_input[:fst_prd]:
                par += ["np.divide(abs(poi_ratio[%s])," % s + str(bias) + ")"]
                par += ["abs(mkt_ratio[%s])" % s]
        exec ("obj=Minimize(max_elemwise(" + ",".join(par) + "))")
        # #        obj=Minimize(max_elemwise(abs(poi_ratio[0]),abs(poi_ratio[1]),abs(poi_ratio[2]),abs(poi_ratio[3]),
        #                                  abs(mkt_ratio[0]),abs(mkt_ratio[1]),abs(mkt_ratio[2]),abs(mkt_ratio[3])))
        prob = Problem(obj, [0 <= f])
        prob.solve()
        rltsc["factor"] = f.value
        for i in range(len(rltsc)):
            rltsc["scen"][i] = ",".join(rltsc["scen"][i])

        df_tmp = spark.createDataFrame(rltsc)

        #rltsc["scen"] = ",".join(rltsc["scen"])
        print index
        if(index == 0):
            df_tmp.write.format("parquet") \
                .mode("overwrite").save(tmp_df_factor_result_path)
        else:
            df_tmp.write.format("parquet") \
                .mode("append").save(tmp_df_factor_result_path)
            #df_result_all = df_result_all.union(df_result)
        time.sleep(0.5)
        index = index + 1
        #df_factor_result = df_factor_result.union(df_tmp)

    return index


def max_outlier_factor_cities_loop(spark, df_result, cities, fst_prd, bias):
    index = 0
    for ct in cities:
        print (u"正在进行 %s factor的计算" % ct)
        df_rlt = df_result.where(df_result.city == ct)
        rlt_scs = df_rlt.select("scen_id").distinct().toPandas()["scen_id"].to_numpy().tolist()
        index = max_outlier_factor_inner_loop_old(spark, rlt_scs, df_rlt.toPandas(), fst_prd, bias, index)

    print (u"已完成 %s factor的计算" % ct)


# 调试Factor 流程
'''
    @fst_prd: 计算factor时，仅考虑前几个产品
    @bias: 计算factor时，偏重于调准mkt的程度（数字越大越准）
'''


def max_outlier_factor(spark, df_result, cities, fst_prd=3, bias=2):
    #prd_input = [u"加罗宁", u"凯纷", u"诺扬"]
    # schema = StructType([
    #     StructField("city", StringType(), True),
    #     StructField("poi", StringType(), True),
    #     StructField("scen_id", IntegerType(), True),
    #     StructField("share", DoubleType(), True),
    #     StructField("num_ot", DoubleType(), True),
    #     StructField("vol_ot", DoubleType(), True),
    #     StructField("poi_vol", DoubleType(), True),
    #     StructField("mkt_vol", DoubleType(), True),
    #     StructField("scen", StringType(), True),
    #     StructField("sales_pnl", DoubleType(), True),
    #     StructField("sales_pnl_mkt", DoubleType(), True),
    #     StructField("ims_share", DoubleType(), True),
    #     StructField("ims_poi_vol", DoubleType(), True),
    #     StructField("ims_mkt_vol", DoubleType(), True),
    #     StructField("factor", DoubleType(), True),
    # ])
    # df_factor_result = spark.createDataFrame([], schema)
    max_outlier_factor_cities_loop(spark, df_result, cities, fst_prd, bias)

    df_factor_result = spark.read.parquet(tmp_df_factor_result_path)
    df_factor_result = df_factor_result.withColumn("poi_tmp",
                                                   ((df_factor_result.poi_vol - df_factor_result.sales_pnl) *
                                                    df_factor_result.factor + df_factor_result.sales_pnl)) \
        .withColumn("mkt_tmp",
                    ((df_factor_result.mkt_vol - df_factor_result.sales_pnl_mkt) *
                     df_factor_result.factor + df_factor_result.sales_pnl_mkt))

    df_factor_result = df_factor_result \
        .withColumn("poi_ratio", df_factor_result.poi_tmp / df_factor_result.ims_poi_vol - 1) \
        .withColumn("mkt_ratio", df_factor_result.mkt_tmp / df_factor_result.ims_mkt_vol - 1) \
        .withColumn("share_factorized", df_factor_result.poi_tmp / df_factor_result.mkt_tmp) \
        .withColumn("share_gap", (df_factor_result.poi_tmp / df_factor_result.mkt_tmp) - df_factor_result.ims_share)

    # df_factor_result.show()

    df_factor_result = df_factor_result.withColumn("rel_gap", df_factor_result.share_gap / df_factor_result.ims_share)
    # brf 的那个行转列我就不写了 @luke
    df_rlt_brf = df_factor_result.select("city", "ims_mkt_vol", "scen", "scen_id", "num_ot", "mkt_ratio", "rel_gap",
                                         "poi")
    return [df_factor_result, df_rlt_brf]
