# coding=utf-8
import itertools
import pandas as pd
import numpy as np
from pyspark.sql import functions as func
from pyspark.sql.types import *

from phCalMktUdf import cal_mkt
from phscenot import max_outlier_seg_scen_ot_spark
from phscenot2 import max_outlier_seg_scen_ot_spark_2
from phsegwoot import max_outlier_seg_wo_ot_spark, max_outlier_seg_wo_ot_old
from phOutlierParameters import prd_input, tmp_df_result_path

'''
    @num_ot_max: 为每个城市选择outlier的数量上限
    @smpl_max: 选outlier的范围，该城市最大的smpl_max家医院
    @fst_prd: 计算factor时，仅考虑前几个产品
    @bias: 计算factor时，偏重于调准mkt的程度（数字越大越准）
'''


def max_outlier_city_loop_template(spark, df_EIA_res, df_seg_city, cities, num_ot_max = 8,smpl_max = 8):
    # schema = StructType([
    #     StructField("poi", StringType(), True),
    #     StructField("scen_id", IntegerType(), True),
    #     StructField("share", DoubleType(), True),
    #     StructField("num_ot", DoubleType(), True),
    #     StructField("vol_ot", DoubleType(), True),
    #     StructField("poi_vol", DoubleType(), True),
    #     StructField("mkt_vol", DoubleType(), True),
    #     StructField("scen", StringType(), True),
    #     StructField("city", StringType(), True)
    # ])
    # df_result_all = spark.createDataFrame([], schema)
    index = 0
    for ct in cities:
        print(ct)
        # 通过Seg来过滤数据
        df_seg_city_iter = df_seg_city.where(df_seg_city.City == ct).select("Seg").distinct()
        df_EIA_res_iter = df_EIA_res.join(df_seg_city_iter, on=["Seg"], how="inner")
        # df_EIA_res_iter = df_EIA_res_iter.withColumn("mkt_size",
        #                                              df_EIA_res_iter["加罗宁"] + df_EIA_res_iter["凯纷"] +
        #                                              df_EIA_res_iter["诺扬"] + df_EIA_res_iter["其它"])
        df_EIA_res_iter = cal_mkt(prd_input, df_EIA_res_iter)

        # 策略 1: 选择最大的Seg
        # TODO: 策略2 我没写，@luke
        # print ct
        ot_seg = df_EIA_res_iter.where(df_EIA_res_iter.PANEL == 1) \
            .groupBy("Seg").sum("Est_DrugIncome_RMB") \
            .orderBy(func.desc("sum(Est_DrugIncome_RMB)")).toPandas()["Seg"].to_numpy()[0]
        # print ot_seg

        df_ot_city = df_EIA_res_iter.where(df_EIA_res_iter.PANEL == 1).select("Seg", "HOSP_ID").distinct()
        # df_ot_city.show()

        # 这部分计算量很小，在Driver机器单线程计算。
        ot_city = df_ot_city.toPandas().values
        cd_arr = {}
        scen = {}
        for i in range(len(ot_city)):
            if ot_city[i][0] in cd_arr:
                cd_arr[ot_city[i][0]] = cd_arr[ot_city[i][0]] + [ot_city[i][1]]
            else:
                cd_arr[ot_city[i][0]] = [ot_city[i][1]]

            scen[ot_city[i][0]] = []

        # print cd_arr
        # print scen

        # 这是一个验证，需要@luke跟进
        # df_drug_income = df_EIA_res_iter\
        #     .where(
        #         (df_EIA_res_iter.Date == 201801) &
        #         (df_EIA_res_iter.HOSP_ID.isin(cd_arr[ot_seg]))
        #     )\
        #     .select("Est_DrugIncome_RMB")
        # df_drug_income.show()

        # 当医院的最大数量大于规定数量，取销量最多的医院
        if len(cd_arr[ot_seg]) > smpl_max:
            # smpl=np.random.choice(cd_arr[ot_seg], smpl_max,p=DrugIncome_std, replace=False).tolist()
            # print ct
            if ct in [u"珠三角市", u"福厦泉市"]:
                # print u"珠福特殊条件"
                df_tmp = df_EIA_res_iter.where(
                    (df_EIA_res_iter.HOSP_ID.isin(cd_arr[ot_seg])) &
                    (df_EIA_res_iter.Date == 201901) &
                    (df_EIA_res_iter.City == ct)
                )
            else:
                df_tmp = df_EIA_res_iter.where(
                    (df_EIA_res_iter.HOSP_ID.isin(cd_arr[ot_seg])) &
                    (df_EIA_res_iter.Date == 201901)
                )

            smpl = df_tmp.orderBy(df_tmp.Est_DrugIncome_RMB.desc()) \
                .limit(smpl_max).toPandas()["HOSP_ID"].to_numpy()
        else:
            smpl = cd_arr[ot_seg]

        # print smpl

        iter_scen = min(num_ot_max + 1, len(cd_arr[ot_seg]))

        for L in range(iter_scen):
            for subset in itertools.combinations(smpl, L):
                scen[ot_seg].append(list(subset))

        df_panel = df_EIA_res_iter.where(df_EIA_res_iter.PANEL == 1).select("HOSP_ID").distinct()

        df_EIA_res_cur = df_EIA_res_iter.where(df_EIA_res_iter.Seg == ot_seg)

        # TODO: ot_seg 可能不存在我日
        seg_wo_ot = df_EIA_res_iter.where(df_EIA_res_iter.Seg != ot_seg) \
            .select("Seg").distinct().toPandas()["Seg"].to_numpy()
        # print seg_wo_ot
        # print ot_seg

        # [other_seg_oth, other_seg_poi] = max_outlier_seg_wo_ot_old(spark, df_EIA_res_iter, ct, seg_wo_ot)
        # print other_seg_poi
        # print other_seg_oth
        [other_seg_oth, other_seg_poi] = max_outlier_seg_wo_ot_spark(spark, df_EIA_res_iter, ct, seg_wo_ot)
        # print other_seg_oth
        # print other_seg_poi
        # df_result = max_outlier_seg_scen_ot_spark(spark, df_EIA_res_cur,
        #                                           df_panel, ct, scen,
        #                                           ot_seg, other_seg_poi, other_seg_oth)

        df_result = max_outlier_seg_scen_ot_spark_2(spark, df_EIA_res_cur,
                                                  df_panel, ct, scen,
                                                  ot_seg, other_seg_poi, other_seg_oth)
        
        # TODO: 这个地方这样写可能会有大量的内存压力，
        # 因为你不知道数据是不是在计算其他数据的过程中已经被释放掉啦内存，
        # 因此会造成大量的重复计算
        # 正确的写法有两种：
        # 1. 每一个城市生成一个固定路径的中间零时文件，最后将零时文件整合成一个文件
        # 2. 利用流式数据，将所有见过append到一个文件中，两种都是可以的

        if(index == 0):
            #df_result_all = df_result
            df_result.write.format("parquet") \
                .mode("overwrite").save(tmp_df_result_path)
        else:
            df_result.write.format("parquet") \
                .mode("append").save(tmp_df_result_path)
            #df_result_all = df_result_all.union(df_result)
        index = index + 1
        #df_result.show()
    df_result_all = spark.read.parquet(tmp_df_result_path)
    return df_result_all
