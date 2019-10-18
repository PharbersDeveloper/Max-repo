# -*- coding: utf-8 -*-
"""
Created on Sun Aug 07 17:11:56 2016
继续 outlier_xxx_multiprod
@author: w520
"""

"""
import selected ot scens 
"""
#改参数
path=u'E:/MAX/Tide/MODEL/凯纷'
path2=u'E:/MAX/Tide/MODEL/凯纷/000ref/'

#改参数
ot_scen = pd.ExcelFile(path+'/020EDA/'+u'Tide_ot_scen_凯纷_ot.xlsx',encoding="GBK").parse("ot")
ot_source = pd.ExcelFile(path+'/020EDA/'+u'Tide_ot_scen_凯纷_ot.xlsx',encoding="GBK").parse(u"凯纷")
ot_pha=[]
import ast
for scen in ot_source.merge(ot_scen)["scen"].map(ast.literal_eval):
    ot_pha+=scen

ot_pha=list(set(ot_pha))
#改参数
uni_ot=xls_un.parse()
uni_ot.loc[uni_ot["Panel_ID"].isin(ot_pha),"PANEL"]=0

#QC
uni_ot.loc[uni_ot["Panel_ID"].isin(ot_pha)].shape

#输出
#改参数
writer=ExcelWriter(path+'/030ot_input/'+u'universe_ot'+u"_凯纷"+'.xlsx')
uni_ot \
            .to_excel(writer, sheet_name=u'凯纷',encoding="GBK", index=False)
writer.save()                 

#MAX 放大计算....
#读入放大后结果
#改参数
max_out_wo_ot = pd.ExcelFile(path+'/040max_output/'+u'凯纷_MAX_out_wo_ot.xlsx',encoding="GBK").parse("Sheet1")
max_out_wo_ot["Predict_Sales"].sum()
max_out_wo_ot = max_out_wo_ot.loc[~(max_out_wo_ot["Prod_Name"] 
                             .apply(lambda x: x[-6:])==u"Others"),:]
max_out_wo_ot["POI"]=max_out_wo_ot["Prod_Name"].apply(lambda x: prod.get(x[:ini],u"其它"))
max_out_wo_ot["City"]=max_out_wo_ot["City"].apply(lambda x:u"福厦泉市" if x in [u"福州市",u"厦门市",u"泉州市"] else u"珠三角市" if x in [u"珠海市",u"东莞市",u"中山市",u"佛山市"] else x)
max_out_wo_ot.head()
max_out_wo_ot.shape
max_out_wo_ot2=max_out_wo_ot[~max_out_wo_ot["Panel_ID"].isin(ot_pha)].groupby(["City","POI"])["Predict_Sales"].sum().reset_index()
#max_out_wo_ot[~max_out_wo_ot["Panel_ID"].isin(ot_pha)]["Predict_Sales"].sum()
#max_out_wo_ot["Predict_Sales"].sum()
#max_out[max_out["city"]==u"常州市"]["sales"].sum()
#max_out2[max_out2["city"]==ict]
max_out_wo_ot2.columns=pd.Index(["city","poi","sales"])

panel_ot=data_EIA4[data_EIA4["HOSP_ID"].isin(ot_pha)].groupby(["City"])[prd_input+[u"其它"]].sum().stack().reset_index()
panel_ot.columns=pd.Index(["city","poi","sales"])

max_out=pd.concat([max_out_wo_ot2,panel_ot],ignore_index=True).groupby(["city","poi"],as_index=False)["sales"].sum()
max_out2=max_out.merge(ims_shr2[ims_shr2["poi"].isin(prd_input)],how="outer").merge(pnl2,on=["city","poi"],how="left")

max_out3=max_out2.groupby("city",as_index=False)["sales"].sum().rename(columns={"sales":"mkt_vol"}).merge(max_out2[max_out2["poi"]!=u"其它"],on="city").rename(columns={"sales":"poi_vol"})


all_cities=ims_shr2[u"city"].drop_duplicates().reset_index(drop=True)
#max_out3[max_out3["city"]==u"大连市"]
#删除南阳
#del all_cities[5]
#del all_cities[12]
#del all_cities[22]
"""
放宽约束
"""
#在首次输出后，如遇需要调整factor的情况,则要取消下面注释过的程序，重新运行下面的部分
#adj_ct=[]
##仅调整mkt ratio的城市
#adj_ct2=[u'上海市',
#u'南宁市',
#u'太原市',
#u'宁波市',
#u'常州市',
#u'成都市',
#u'深圳市',
#u'珠三角市']
#adj_ct=[u"常州市",u"成都市",u"哈尔滨市",u"济宁市",u"临沂市",u"南阳市",u"绍兴市",u"西安市",u"烟台市",u"徐州市",u"扬州市",u"长沙市",u"珠三角市",u"潍坊市",u"无锡市"]
#adj_ct2=[u'南通市',u'哈尔滨市',u'太原市',u'宁波市',u'扬州市',u'无锡市',u'昆明市',u'深圳市',u'青岛市',u'深圳市']
adj_ct=[];adj_ct2=[u'无锡市']
"""
放宽限制留下的产品序列
"""
'''
计算factor时，仅考虑前几个产品
'''
fst_prd=2

'''
计算factor时，偏重于调准mkt的程度（数字越大越准）
'''
bias=2
release_id=0
for ict in all_cities:
#    print ict
#   ict=u"西安市"
    print u"正在进行 %s factor的计算" % ict
    rltsc=max_out3[max_out3["city"]==ict].reset_index(drop=False).fillna(0)

    f= Variable()
    poi_ratio={}
    mkt_ratio={}
    share_gap={}
    for iprd in range(len(rltsc.index)): 
        poi_ratio[iprd]=np.divide((rltsc["poi_vol"][iprd]-rltsc["sales_pnl"][iprd])*f+rltsc["sales_pnl"][iprd],rltsc["ims_poi_vol"][iprd])-1
        mkt_ratio[iprd]=np.divide((rltsc["mkt_vol"][iprd]-rltsc["sales_pnl_mkt"][iprd])*f+rltsc["sales_pnl_mkt"][iprd],rltsc["ims_mkt_vol"][iprd])-1
    par=[]
    for s in range(len(rltsc.index)):
        if rltsc["poi"][s] in prd_input[:fst_prd]:
            par+= ["np.divide(abs(poi_ratio[%s])," % s+str(bias)+")"]
            par+= ["abs(mkt_ratio[%s])" % s]            
        
    if ict in adj_ct:
        print u"放宽"+ict+u"的限制"
        cstm_id=rltsc["poi"][rltsc["poi"]==prd_input[release_id]].index[0]
#        print prd_input[release_id]
#        obj=Minimize(max_elemwise(abs(poi_ratio[cstm_id]),abs(mkt_ratio[cstm_id])))
        obj=Minimize(max_elemwise(abs(poi_ratio[cstm_id])/bias,abs(mkt_ratio[cstm_id])))
    elif ict in adj_ct2:
        print u"放宽"+ict+u"的限制II"
        cstm_id=rltsc["poi"][rltsc["poi"]==prd_input[release_id]].index[0]        
        obj=Minimize(max_elemwise(0.05,abs(mkt_ratio[cstm_id])))
    else:
        print u"未放宽"+ict+u"的限制"
        exec ("obj=Minimize(max_elemwise("+",".join(par)+"))") 
#        obj=Minimize(max_elemwise(abs(poi_ratio[0]),abs(poi_ratio[1]),abs(poi_ratio[2]),abs(poi_ratio[3]), 
#                                  abs(mkt_ratio[0]),abs(mkt_ratio[1]),abs(mkt_ratio[2]),abs(mkt_ratio[3])))         
    prob = Problem(obj, [f>0])
    prob.solve()
    max_out3.loc[(max_out3["city"]==ict),"factor"]=f.value
    for p,iprd in enumerate(rltsc["poi"]): 
        max_out3.loc[(max_out3["city"]==ict)&(max_out3["poi"]==iprd),"poi_ratio"]=np.divide((rltsc["poi_vol"][p]-rltsc["sales_pnl"][p])*f.value+rltsc["sales_pnl"][p],rltsc["ims_poi_vol"][p])-1
        max_out3.loc[(max_out3["city"]==ict)&(max_out3["poi"]==iprd),"mkt_ratio"]=np.divide((rltsc["mkt_vol"][p]-rltsc["sales_pnl_mkt"][p])*f.value+rltsc["sales_pnl_mkt"][p],rltsc["ims_mkt_vol"][p])-1
        max_out3.loc[(max_out3["city"]==ict)&(max_out3["poi"]==iprd),"share_factorized"]=np.divide((rltsc["poi_vol"][p]-rltsc["sales_pnl"][p])*f.value+rltsc["sales_pnl"][p],(rltsc["mkt_vol"][p]-rltsc["sales_pnl_mkt"][p])*f.value+rltsc["sales_pnl_mkt"][p])
        max_out3.loc[(max_out3["city"]==ict)&(max_out3["poi"]==iprd),"share_gap"]=np.divide((rltsc["poi_vol"][p]-rltsc["sales_pnl"][p])*f.value+rltsc["sales_pnl"][p],(rltsc["mkt_vol"][p]-rltsc["sales_pnl_mkt"][p])*f.value+rltsc["sales_pnl_mkt"][p])-rltsc[rltsc["poi"]==iprd]["ims_share"].tolist()[0]

    print u"已完成 %s factor的计算" % ict

max_out3["rel_gap"]=np.divide(max_out3["share_gap"],max_out3["ims_share"])
print max_out3.shape
#改参数
writer=ExcelWriter(path+'/040max_output/'+u'max_out_sum'+u"_凯纷"+'.xlsx')
max_out3[max_out3["city"].isin(all_cities)].to_excel(writer, sheet_name=u'凯纷',encoding="GBK", index=False)
writer.save()                 