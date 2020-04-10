# coding=utf-8


doi = u"NN7"

cities = [
    u"北京市", \
    u"常州市", \
    u"成都市", \
    u"大连市", \
    u"大庆市", \
    u"福厦泉市", \
    u"广州市", \
    u"贵阳市", \
    u"哈尔滨市", \
    u"海口市", \
    u"杭州市", \
    u"合肥市", \
    u"呼和浩特市", \
    u"济南市", \
    u"济宁市", \
    u"金台嘉绍", \
    u"昆明市", \
    u"兰州市", \
    u"洛阳市", \
    u"南昌市", \
    u"南京市", \
    u"南宁市", \
    u"南通市", \
    u"南阳市", \
    u"宁波市", \
    u"平顶山市", \
    u"齐齐哈尔市", \
    u"青岛市", \
    u"上海市", \
    u"深圳市", \
    u"沈阳市", \
    u"石家庄市", \
    u"苏州市", \
    u"太原市", \
    u"天津市", \
    u"温州市", \
    u"乌鲁木齐市", \
    u"无锡市", \
    u"武汉市", \
    u"西安市", \
    u"徐州市", \
    u"银川市", \
    u"长春市", \
    u"长沙市", \
    u"郑州市", \
    u"重庆市", \
    u"珠三角市", \
    u"淄博市"]


uni_path = u"/common/projects/max/NN/Universe/Universe_NN2-7"
#seg_city_path = u"starlord/user/alfredyang/outlier/seg_city"
#EIA_res_path = u"starlord/user/alfredyang/outlier/EIA_res"
pnl_path = u"/common/projects/max/NN/panel-result_NN_201801-201912"
ims_path = u"/common/projects/max/NN/ims_info/"+doi+"_ims_info_1901-1912"
df_factor_result_path = u"/common/projects/max/NN1/outlier/"+doi+"_df_factor_result"
df_rlt_brf_path = u"/common/projects/max/NN1/outlier/"+doi+"_df_rlt_brf"
tmp_df_result_path = u"/common/projects/max/NN1/outlier/"+doi+"tmp_df_result"
tmp_df_factor_result_path = u"/common/projects/max/NN1/outlier/"+doi+"tmp_df_factor_result"

prd_input = [u"诺和锐 30"]
prod=dict([(prd_input[p],"prd"+str(p)) for p in range(len(prd_input))])
sql_content = '''select `mkt_vol`,
                     stack(1, '诺和锐 30', `诺和锐 30`) as (`poi`, `poi_vol` )
                     from  v_pivot             
                  '''

sql_content2 = '''select `mkt_vol`, `scen_id`, `scen`, `city`, `num_ot`, `vol_ot`,
                 stack(1, '诺和锐 30', `诺和锐 30`) as (`poi`, `poi_vol` )
                 from  v_pivot
              '''

