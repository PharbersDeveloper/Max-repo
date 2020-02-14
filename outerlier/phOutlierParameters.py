# coding=utf-8


doi = u"SNY8"

cities = [u"长春市", \
          u"北京市", \
          u"长沙市", \
          u"成都市", \
          u"重庆市", \
          u"大连市", \
          u"福厦泉市", \
          u"广州市", \
          u"贵阳市", \
          u"杭州市", \
          u"哈尔滨市", \
          u"济南市", \
          u"昆明市", \
          u"兰州市", \
          u"南昌市", \
          u"南京市", \
          u"珠三角市", \
          u"青岛市", \
          u"上海市", \
          u"沈阳市", \
          u"深圳市", \
          u"石家庄市", \
          u"太原市", \
          u"天津市", \
          u"武汉市", \
          u"乌鲁木齐市", \
          u"西安市", \
          u"徐州市", \
          u"郑州市"]




uni_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/universe_az_sanofi_base"
#seg_city_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/seg_city"
#EIA_res_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/EIA_res"
pnl_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/panel-result_AZ_Sanofi_201701-201911_20200212"
ims_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/ims_info/"+doi+"_ims_info_1901-1911"
df_factor_result_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/outlier/"+doi+"_df_factor_result"
df_rlt_brf_path = u"hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/outlier/"+doi+"_df_rlt_brf"

prd_input = [u"波立维", u"倍林达", u"泰嘉"]
prod=dict([(prd_input[p],"prd"+str(p)) for p in range(len(prd_input))])
sql_content = '''select `mkt_vol`,
                     stack(3, '波立维', `波立维`, '倍林达', `倍林达`, '泰嘉', `泰嘉`) as (`poi`, `poi_vol` )
                     from  v_pivot             
                  '''

sql_content2 = '''select `mkt_vol`, `scen_id`, `scen`, `city`, `num_ot`, `vol_ot`,
                 stack(3, '波立维', `波立维`, '倍林达', `倍林达`, '泰嘉', `泰嘉`) as (`poi`, `poi_vol` )
                 from  v_pivot
              '''

