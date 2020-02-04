# coding=utf-8

prd_input = [u"加罗宁", u"凯纷", u"诺扬"]
prod=dict([(prd_input[p],"prd"+str(p)) for p in range(len(prd_input))])
cities = [u"北京市"]



uni_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/universe"
#seg_city_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/seg_city"
#EIA_res_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/EIA_res"
pnl_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/凯纷_Panel_2018"
ims_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/凯纷_ims_info18"


sql_content = '''select `mkt_vol`,
                     stack(3, '加罗宁', `加罗宁`, '凯纷', `凯纷`, '诺扬', `诺扬`) as (`poi`, `poi_vol` )
                     from  v_pivot             
                  '''

sql_content2 = '''select `mkt_vol`, `scen_id`, `scen`, `city`, `num_ot`, `vol_ot`,
                 stack(3, '加罗宁', `加罗宁`, '凯纷', `凯纷`, '诺扬', `诺扬`) as (`poi`, `poi_vol` )
                 from  v_pivot
              '''