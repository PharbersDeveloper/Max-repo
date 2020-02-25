# coding=utf-8


doi = u"AZ8"

cities = [u"北京市", \
    u"福厦泉市", \
    u"广州市", \
    u"济南市", \
    u"上海市",\
    u"天津市"]




uni_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/universe_az_sanofi_base"
#seg_city_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/seg_city"
#EIA_res_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/EIA_res"
pnl_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/panel-result_AZ_Sanofi_201701-201911_20200212"
ims_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/ims_info/"+doi+"_ims_info_1901-1911"
df_factor_result_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/outlier/"+doi+"_df_factor_result_5ct"
df_rlt_brf_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/outlier/"+doi+"_df_rlt_brf_5ct"
tmp_df_result_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/outlier/"+doi+"tmp_df_result"
tmp_df_factor_result_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/outlier/"+doi+"tmp_df_factor_result"

prd_input = [u"耐信", u"奥西康", u"泮立苏"]
prod=dict([(prd_input[p],"prd"+str(p)) for p in range(len(prd_input))])
sql_content = '''select `mkt_vol`,
                     stack(3, '耐信', `耐信`, '奥西康', `奥西康`, '泮立苏', `泮立苏`) as (`poi`, `poi_vol` )
                     from  v_pivot             
                  '''

sql_content2 = '''select `mkt_vol`, `scen_id`, `scen`, `city`, `num_ot`, `vol_ot`,
                 stack(3, '耐信', `耐信`, '奥西康', `奥西康`, '泮立苏', `泮立苏`) as (`poi`, `poi_vol` )
                 from  v_pivot
              '''

