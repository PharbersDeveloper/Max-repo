prd_input = [u"加罗宁", u"凯纷", u"诺扬"]
prod=dict([(prd_input[p],"prd"+str(p)) for p in range(len(prd_input))])
cities = [u"北京市"]





uni_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/universe"
seg_city_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/seg_city"
EIA_res_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/EIA_res"
pnl_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/凯纷_Panel_2018"
ims_path = u"hdfs://192.168.100.137/user/alfredyang/outlier/凯纷_ims_info18"