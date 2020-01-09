# coding=utf-8
import itertools
import pandas as pd
import numpy as np

from phsegwoot import max_outlier_seg_wo_ot_spark, max_outlier_seg_wo_ot_old

'''
    @num_ot_max: 为每个城市选择outlier的数量上限
    @smpl_max: 选outlier的范围，该城市最大的smpl_max家医院
    @fst_prd: 计算factor时，仅考虑前几个产品
    @bias: 计算factor时，偏重于调准mkt的程度（数字越大越准）
'''


def max_outlier_city_loop_template(spark, df_EIA_res, df_seg_city, cities, num_ot_max=8, smpl_max=8, fst_prd=3, bias=2):
    for ct in cities:
        # 通过Seg来过滤数据
        df_seg_city_iter = df_seg_city.where(df_seg_city.City == ct).select("Seg").distinct()
        df_EIA_res_iter = df_EIA_res.join(df_seg_city_iter, on=["Seg"], how="inner")
        df_EIA_res_iter = df_EIA_res_iter.withColumn("mkt_size",
                                                     df_EIA_res_iter["加罗宁"] + df_EIA_res_iter["凯纷"] +
                                                     df_EIA_res_iter["诺扬"] + df_EIA_res_iter["其它"]) \
            .where(df_EIA_res_iter["其它"].isNotNull())

        # df_EIA_res_iter.show()

        # 策略 1: 选择最大的Seg
        # TODO: 策略2 我没写，@luke
        print ct
        ot_seg = df_EIA_res_iter.where(df_EIA_res_iter.PANEL == 1) \
            .groupBy("Seg").sum("Est_DrugIncome_RMB") \
            .orderBy("sum(Est_DrugIncome_RMB)").toPandas()["Seg"].to_numpy()[0]
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

        print cd_arr
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
                    (df_EIA_res_iter.Date == 201801) &
                    (df_EIA_res_iter.City == ct)
                )
            else:
                df_tmp = df_EIA_res_iter.where(
                    (df_EIA_res_iter.HOSP_ID.isin(cd_arr[ot_seg])) &
                    (df_EIA_res_iter.Date == 201801)
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
        print other_seg_poi
        print other_seg_oth

        print scen[ot_seg]

        prd_input = ["加罗宁", "凯纷", "诺扬"]
        poi = []
        scen_id = []
        share = []
        num_ot_lst = []
        vol_ot_lst = []
        poi_vol_lst = []
        mkt_vol_lst = []
        scen_lst = []
        # i=0

        df_EIA_res_cur.show()
        data_EIA6 = df_EIA_res_cur.toPandas()
        # print data_EIA6
        panel = df_panel.toPandas()
        #    按outlier的scenario循环
        for i in range(len(scen[ot_seg])):
            #       i=87
            panel_sc = panel[panel["HOSP_ID"].isin(scen[ot_seg][i])]

            num_ot = len(scen[ot_seg][i])
            vol_ot = data_EIA6[data_EIA6["HOSP_ID"].isin(panel_sc["HOSP_ID"])]["Est_DrugIncome_RMB"].sum()

            #       print  cities[j]
            data_EIA7 = data_EIA6[(data_EIA6["BEDSIZE"] >= 100) \
                                  & (data_EIA6["City"] == ct)]
            poi_ot = data_EIA7[data_EIA7 \
                ["HOSP_ID"].isin(panel_sc["HOSP_ID"])][prd_input].sum()

            oth_ot = data_EIA7[data_EIA7["HOSP_ID"].isin(panel_sc["HOSP_ID"])][u"其它"].sum()

            rest = data_EIA6[~data_EIA6["HOSP_ID"].isin(panel_sc["HOSP_ID"])]
            rest_panel = rest[rest["PANEL"] == 1]

            rest_seg = pd.DataFrame(rest.groupby("Date").sum()[prd_input + [u"其它", "Est_DrugIncome_RMB"]])

            rest_seg_p0_bed100 = pd.DataFrame(rest[(rest["BEDSIZE"] >= 100) \
                                                   & (rest["City"] == ct)
                                                   & (rest["PANEL"] == 0)]
                                              .groupby("Date").sum()[prd_input + [u"其它", "Est_DrugIncome_RMB"]])
            rest_seg_p1_bed100 = pd.DataFrame(rest[(rest["BEDSIZE"] >= 100) \
                                                   & (rest["City"] == ct) \
                                                   & (rest["PANEL"] == 1)] \
                                              .groupby("Date").sum()[prd_input + [u"其它", "Est_DrugIncome_RMB"]])
            rest_seg_p1 = pd.DataFrame(rest[(rest["PANEL"] == 1)]
                                       .groupby("Date").sum()[prd_input + [u"其它", "Est_DrugIncome_RMB"]])

            if len(rest_seg_p0_bed100["Est_DrugIncome_RMB"]) == 0:
                rest_seg["w"] = rest_seg["Est_DrugIncome_RMB"] * 0
            else:
                rest_seg["w"] = np.divide(rest_seg_p0_bed100["Est_DrugIncome_RMB"], rest_seg_p1["Est_DrugIncome_RMB"])

            for iprd in prd_input:
                rest_seg[iprd + "_fd"] = rest_seg[iprd].fillna(0) * rest_seg["w"] + rest_seg_p1_bed100[iprd].fillna(0)

            rest_seg["oth_fd"] = rest_seg[u"其它"].fillna(0) * rest_seg["w"] + rest_seg_p1_bed100[u"其它"].fillna(0)

            rest_poi = rest_seg[[iprd + "_fd" for iprd in prd_input]].sum()
            rest_oth = rest_seg["oth_fd"].sum()

            for iprd in prd_input:
                #           print iprd
                poi_vol = rest_poi[iprd + "_fd"] + other_seg_poi[iprd] + poi_ot[iprd]
                mkt_vol = rest_poi.sum() + sum(
                    other_seg_poi.values()) + poi_ot.sum() + rest_oth + other_seg_oth + oth_ot
                mkt_share = np.divide(poi_vol, mkt_vol)

                poi += [iprd]
                scen_id += [i]
                share += [mkt_share]
                num_ot_lst += [num_ot]
                vol_ot_lst += [vol_ot]
                poi_vol_lst += [poi_vol]
                mkt_vol_lst += [mkt_vol]
                scen_lst += [scen[ot_seg][i]]
