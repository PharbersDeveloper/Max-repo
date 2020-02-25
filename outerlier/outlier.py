# -*- coding: utf-8 -*-
"""
Created on Fri Jun 03 11:20:27 2016

@author: bpeng
"""
import itertools
import pandas as pd
from pandas import DataFrame,ExcelWriter
import xlrd, openpyxl 
import numpy as np
from cvxpy import *
'''
工作目录: 1.panel  2.universe  3.IMS v.s. MAX 
'''
#改参数
path=u'E:/MAX/Tide/MODEL/凯纷'
data_EIA = pd.ExcelFile(u'E:/MAX/Tide/MODEL/凯纷/010Panel_data/'+u'凯纷_Panel_2018.xlsx',encoding="GBK").parse()
data_EIA=data_EIA.loc[data_EIA['Date'].apply(lambda x:int(x/100)==2018),]
#data_EIA.loc[data_EIA['Prod_Name']=='OTHERS','Prod_Name']=u'其他'
#data_EIA.loc[data_EIA['Prod_Name']==u'OTHER阿卡波糖','Prod_Name']=u'阿卡波糖OTHER'
data_EIA['Prod_Name'].drop_duplicates()
#改参数
year=2018  

'''
主要产品
'''
#改参数
#前几个字符可以区分？
ini=2
#需要考察的产品
#改参数
prd_input=[u"加罗宁",u"凯纷",u"诺扬"]

prod=dict([(p[0:ini],p) for p in prd_input])

#QC
#print prod.get(u"诺和",u"其它")


'''
需要修改
判断客户产品的条件，例如：x[:2]==u"客户产品名称的前两个字符"
'''
data_EIA.loc[:,"POI"]=data_EIA.loc[:,"Prod_Name"] \
                     .apply(lambda x: prod.get(x[:ini].strip(),u"其它"))
data_EIA.head()
pd.value_counts(data_EIA.loc[:,"POI"])
                   
cols=data_EIA.drop(['Sales','Units'
                           ,"Prod_Name","Prod_CNAME"
                           ,"Strength","DOI","DOIE"], axis=1).columns

data_EIA[cols]=data_EIA[cols].fillna(" ")

data_EIA2=data_EIA.  \
            groupby(cols.values.tolist() \
                                  ).agg({u'Sales': 'sum',u'Units':'sum'})

print data_EIA2["Sales"].sum(),data_EIA["Sales"].sum()                                  
                                  
data_EIA3=data_EIA2["Sales"].unstack("POI").reset_index()
data_EIA3.head()
print data_EIA3[prd_input].sum().sum()+data_EIA3[u"其它"].sum()

#改参数，universe文件的位置，“cv”需替换成工作表名称

path2 = u'E:/MAX/Tide/MODEL/凯纷/000ref/' 
xls_un = pd.ExcelFile(path2+u'universe.xlsx',encoding="GBK")

# universe = xls_un.parse("universe")
universe = xls_un.parse()[["Panel_ID","Seg","City","BEDSIZE","Est_DrugIncome_RMB","PANEL"]]
seg_city = xls_un.parse()[["City","Seg"]].drop_duplicates()
hos_city = xls_un.parse()[["Panel_ID","City"]].drop_duplicates()
#universe=universe[universe["BEDSIZE"]>=100]

date = {'Date' : [year*100+m+1 for m in range(12)]}
date["key"]=1
universe["key"]=1  

universe2=pd.merge(pd.DataFrame(date),universe,on="key",how="outer")
universe2=universe2.rename(columns={u'Panel_ID':u'HOSP_ID'})
print universe.shape,universe2.shape

data_EIA4=pd.merge(universe2,data_EIA3
                   ,on=["HOSP_ID","Date"]
                   ,how="left",suffixes={"","_b"})
print data_EIA4[prd_input].sum().sum()+data_EIA4[u"其它"].sum(), data_EIA2["Sales"].sum()
print data_EIA4.head()
#PANEL的部分医院不在universe中
data_EIA3[~data_EIA3["HOSP_ID"].isin(universe["Panel_ID"])]

#对福厦泉、珠三角的调整需要：

data_EIA4["City"]=data_EIA4["City"].apply(lambda x:u"福厦泉市" if x in [u"福州市",u"厦门市",u"泉州市"] else u"珠三角市" if x in [u"珠海市",u"东莞市",u"中山市",u"佛山市"] else x)
seg_city["City"]=seg_city["City"].apply(lambda x:u"福厦泉市" if x in [u"福州市",u"厦门市",u"泉州市"] else u"珠三角市" if x in [u"珠海市",u"东莞市",u"中山市",u"佛山市"] else x)
seg_city=seg_city.drop_duplicates()
hos_city["City"]=hos_city["City"].apply(lambda x:u"福厦泉市" if x in [u"福州市",u"厦门市",u"泉州市"] else u"珠三角市" if x in [u"珠海市",u"东莞市",u"中山市",u"佛山市"] else x)

"""
PANEL医院 数据整理，用于factor计算
"""
panel_hos=universe[universe["PANEL"]==1]["Panel_ID"]

pnl= \
data_EIA[data_EIA["HOSP_ID"].isin(panel_hos)] \
        .merge(hos_city,left_on="HOSP_ID",right_on="Panel_ID",how="left") \
        .groupby(["City","POI"])["Sales"].sum()
pnl=pnl.unstack("POI").fillna(0).stack()
pnl.name="Sales"

pnl2= \
pd.DataFrame(pnl.sum(level="City")).reset_index() \
        .merge(pd.DataFrame(pnl).reset_index(),
               on="City",
               how="right",
               suffixes=["_pnl_mkt","_pnl"])
pnl2.columns=pd.Index(pnl2.columns.str.lower())               

'''
#ims对比max的文件

'''
#xls_un = pd.ExcelFile(path+'/'+u'harnal_market_share.xlsx',encoding="GBK")
#compare = xls_un.parse(u"harnal_market_share")



'''
ims各城市各产品市场份额
'''
ims_shr = pd.ExcelFile(path+'/011IMS_data/'+u'凯纷_ims_info18.xlsx',encoding="GBK") \
            .parse()[["city","poi","ims_share","ims_poi_vol"]]
ims_shr = ims_shr[ims_shr["city"]!="CHPA"]
cities=ims_shr[u"city"].drop_duplicates().reset_index(drop=True)


poi = {'poi' : prd_input}
poi["key"]=1
ct=pd.DataFrame(cities)
ct["key"]=1
ct_pd=pd.merge(ct,pd.DataFrame(poi),on="key",how="outer").drop("key",axis=1) \
        .assign(**dict.fromkeys(["ims_share","ims_poi_vol"],0))

ims_shr2=ims_shr \
             .groupby("city",as_index=False)["ims_poi_vol"].sum().rename(columns={"ims_poi_vol":"ims_mkt_vol"}) \
             .merge(pd.concat([ims_shr,ct_pd],ignore_index=True).groupby(["city","poi"],as_index=False).sum())


'''
需要在list中选择 需要挑选outlier 的城市
'''
#改参数，首次运行程序需注释掉下面两行，在其后挑选outlier的时候，再限制cities的范围
#lst=[]
#
#
#lst=[u'北京市',u'常州市',u'福厦泉市',u'广州市',u'福厦泉市',u'广州市',u'宁波市',u'上海市',\
#u'苏州市',u'天津市',u'温州市',u'无锡市',u'西安市',u'郑州市',u'珠三角市']


#cities=cities[cities.isin(lst)].reset_index(drop=True)
#cities=cities.iloc[[2,3,5]].reset_index(drop=True)


'''
为每个城市选择outlier的数量上限
'''
num_ot_max=8

'''
选outlier的范围，该城市最大的smpl_max家医院
'''
smpl_max=8

'''
计算factor时，仅考虑前几个产品
'''
fst_prd=3

'''
计算factor时，偏重于调准mkt的程度（数字越大越准）
'''
bias=2

for j in range(len(cities)):
#    j=0
    data_EIA5=data_EIA4[data_EIA4.loc[:,"Seg"] 
                        .isin(seg_city[seg_city["City"]==cities[j]].iloc[:,1])]
    
    print cities[j],u"所在seg的样本医院数量:",data_EIA5["PANEL"][data_EIA5["Date"]==(201801)].sum()
    
    #    ot_seg=pd.value_counts(data_EIA5[data_EIA5["PANEL"]==1]["Seg"]).idxmax()
    data_EIA5["mkt_size"]=data_EIA5[prd_input].sum(axis=1).fillna(0)+data_EIA5[u"其它"].fillna(0)
    data_EIA5[~data_EIA5[u"其它"].isnull()]
#    data_EIA5.columns
#    策略#1
    ot_seg=data_EIA5[data_EIA5["PANEL"]==1].groupby("Seg")["Est_DrugIncome_RMB"].sum().idxmax()
#    ot_seg=100
#    策略#2
#    ot_seg=data_EIA5[data_EIA5["PANEL"]==1].groupby("Seg")["PANEL"].sum().idxmax()
#    ot_seg=7
    print cities[j],u"选中seg的样本医院数量:",data_EIA5["PANEL"] \
                                                      [(data_EIA5["Date"]==(201801))&(data_EIA5["Seg"]==ot_seg)].sum()
    
    ot_city=data_EIA5[data_EIA5["PANEL"]==1][["Seg","HOSP_ID"]].drop_duplicates().values
    cd_arr={}
    scen={}
    for i in range(len(ot_city)):
        if ot_city[i][0] in cd_arr:
            cd_arr[ot_city[i][0]]=cd_arr[ot_city[i][0]]+[ot_city[i][1]]
        else:
            cd_arr[ot_city[i][0]]=[ot_city[i][1]]  
            
        scen[ot_city[i][0]]=[]
        
    DrugIncome=data_EIA5[data_EIA5["Date"]==(201801)][data_EIA5["HOSP_ID"].isin(cd_arr[ot_seg])]["Est_DrugIncome_RMB"].tolist()
    DrugIncome_std=np.divide(DrugIncome, sum(DrugIncome))    
    if len(cd_arr[ot_seg])>smpl_max :
#        smpl=np.random.choice(cd_arr[ot_seg], smpl_max,p=DrugIncome_std, replace=False).tolist()
        if cities[j] in [u"珠三角市",u"福厦泉市"]:
            print u"珠福特殊条件"
            cond=(data_EIA5["HOSP_ID"].isin(cd_arr[ot_seg]))&(data_EIA5["Date"]==(201801))&(data_EIA5["City"]==cities[j])
        else:
            cond=(data_EIA5["HOSP_ID"].isin(cd_arr[ot_seg]))&(data_EIA5["Date"]==(201801))
            
        dftmp=data_EIA5[cond]
        smpl=dftmp.sort_values(['Est_DrugIncome_RMB'], ascending=False)["HOSP_ID"].tolist()[:smpl_max]
    else:
        smpl=cd_arr[ot_seg]
    
    
    iter_scen=min(num_ot_max+1,len(cd_arr[ot_seg]))
    
    for L in range(iter_scen):
        for subset in itertools.combinations(smpl, L):
            scen[ot_seg].append(list(subset))
    
    
    panel=pd.DataFrame(data_EIA5[data_EIA5["PANEL"]==1]["HOSP_ID"]).drop_duplicates()
    
       
    data_EIA6=data_EIA5[data_EIA5["Seg"]==ot_seg]
    
    seg_wo_ot=data_EIA5["Seg"][data_EIA5["Seg"]!=ot_seg].drop_duplicates().reset_index(drop=True)
    
    
    seg_wo_ot_poi={}
    seg_wo_ot_oth={}
    other_seg_poi={}
    for z in range(len(seg_wo_ot)):
#        z=0
        other_seg=data_EIA5[data_EIA5["Seg"]==seg_wo_ot[z]]
        other_seg_panel=data_EIA5[(data_EIA5["Seg"]==seg_wo_ot[z])&(data_EIA5["PANEL"]==1)]
        
        oth_seg=pd.DataFrame(other_seg.groupby("Date").sum()[prd_input+[u"其它","Est_DrugIncome_RMB"]])

        oth_seg_p0_bed100=pd.DataFrame(other_seg[(other_seg["BEDSIZE"]>=100) \
                                          &(other_seg["City"]==cities[j])
                                          &(other_seg["PANEL"]==0)]
                                    .groupby("Date").sum()[prd_input+[u"其它","Est_DrugIncome_RMB"]])
        oth_seg_p1_bed100=pd.DataFrame(other_seg[(other_seg["BEDSIZE"]>=100) \
                                          &(other_seg["City"]==cities[j])
                                          &(other_seg["PANEL"]==1)]
                                    .groupby("Date").sum()[prd_input+[u"其它","Est_DrugIncome_RMB"]])
        
        oth_seg_p1       =pd.DataFrame(other_seg[(other_seg["PANEL"]==1)]
                                    .groupby("Date").sum()[prd_input+[u"其它","Est_DrugIncome_RMB"]])    
        
        if len(oth_seg_p0_bed100["Est_DrugIncome_RMB"])==0:
            oth_seg["w"]=oth_seg["Est_DrugIncome_RMB"]*0
        else:
            oth_seg["w"]=np.divide(oth_seg_p0_bed100["Est_DrugIncome_RMB"],oth_seg_p1["Est_DrugIncome_RMB"])
        
        if len(oth_seg_p1_bed100["Est_DrugIncome_RMB"])==0:
            oth_seg_p1_bed100[prd_input+[u"其它"]]=oth_seg[prd_input+[u"其它"]].fillna(0)*0
        
        oth_seg["oth_fd"]=oth_seg[u"其它"].fillna(0)*oth_seg["w"]+oth_seg_p1_bed100[u"其它"].fillna(0)

        for iprd in prd_input:
            oth_seg[iprd+"_fd"]=oth_seg[iprd].fillna(0)*oth_seg["w"]+oth_seg_p1_bed100[iprd].fillna(0)
            if seg_wo_ot_poi.get(iprd):
                seg_wo_ot_poi[iprd].update({z:oth_seg[iprd+"_fd"].sum()})
            else:
                seg_wo_ot_poi[iprd]={z:oth_seg[iprd+"_fd"].sum()}
        
        seg_wo_ot_oth[z]=oth_seg[u"oth_fd"].sum()
    
    for iprd in prd_input:
        other_seg_poi[iprd]=sum(seg_wo_ot_poi[iprd].values())
    
    other_seg_oth=sum(seg_wo_ot_oth.values())
    
    poi=[]
    scen_id=[]
    share=[]
    num_ot_lst=[]
    vol_ot_lst=[]
    poi_vol_lst=[]
    mkt_vol_lst=[]
    scen_lst=[]
    # i=0
    
#    按outlier的scenario循环
    for i in range(len(scen[ot_seg])):
#       i=87       
        panel_sc=panel[panel["HOSP_ID"].isin(scen[ot_seg][i])]
        
        num_ot=len(scen[ot_seg][i])
        vol_ot=data_EIA6[data_EIA6["HOSP_ID"].isin(panel_sc["HOSP_ID"])]["Est_DrugIncome_RMB"].sum()
        
#       print  cities[j]
        data_EIA7=data_EIA6[(data_EIA6["BEDSIZE"]>=100) \
                            &(data_EIA6["City"]==cities[j])]    
        poi_ot=data_EIA7[data_EIA7   \
                        ["HOSP_ID"].isin(panel_sc["HOSP_ID"])][prd_input].sum()
        
        oth_ot=data_EIA7[data_EIA7["HOSP_ID"].isin(panel_sc["HOSP_ID"])][u"其它"].sum()
        
        
        rest=data_EIA6[~data_EIA6["HOSP_ID"].isin(panel_sc["HOSP_ID"])]
        rest_panel=rest[rest["PANEL"]==1]
        
        rest_seg=pd.DataFrame(rest.groupby("Date").sum()[prd_input+[u"其它","Est_DrugIncome_RMB"]])
      
     
        rest_seg_p0_bed100=pd.DataFrame(rest[(rest["BEDSIZE"]>=100) \
                                          &(rest["City"]==cities[j])
                                          &(rest["PANEL"]==0)]
                                    .groupby("Date").sum()[prd_input+[u"其它","Est_DrugIncome_RMB"]])
        rest_seg_p1_bed100=pd.DataFrame(rest[(rest["BEDSIZE"]>=100) \
                                          &(rest["City"]==cities[j]) \
                                          &(rest["PANEL"]==1)] \
                                    .groupby("Date").sum()[prd_input+[u"其它","Est_DrugIncome_RMB"]])       
        rest_seg_p1       =pd.DataFrame(rest[(rest["PANEL"]==1)]
                                    .groupby("Date").sum()[prd_input+[u"其它","Est_DrugIncome_RMB"]])    
        
        if len(rest_seg_p0_bed100["Est_DrugIncome_RMB"])==0:
            rest_seg["w"]=rest_seg3["Est_DrugIncome_RMB"]*0
        else:
            rest_seg["w"]=np.divide(rest_seg_p0_bed100["Est_DrugIncome_RMB"],rest_seg_p1["Est_DrugIncome_RMB"])
        
        for iprd in prd_input:
            rest_seg[iprd+"_fd"]=rest_seg[iprd].fillna(0)*rest_seg["w"]+rest_seg_p1_bed100[iprd].fillna(0)
            
        rest_seg["oth_fd"]=rest_seg[u"其它"].fillna(0)*rest_seg["w"]+rest_seg_p1_bed100[u"其它"].fillna(0)
        
        rest_poi=rest_seg[[iprd+"_fd" for iprd in prd_input]].sum()
        rest_oth=rest_seg["oth_fd"].sum()
    
        for iprd in prd_input:
#           print iprd
            poi_vol=rest_poi[iprd+"_fd"]+other_seg_poi[iprd]+poi_ot[iprd]
            mkt_vol=rest_poi.sum()+sum(other_seg_poi.values())+poi_ot.sum()+rest_oth+other_seg_oth+oth_ot
            mkt_share=np.divide(poi_vol,mkt_vol)
            

            poi+=[iprd]
            scen_id+=[i]
            share+=[mkt_share]
            num_ot_lst+=[num_ot]
            vol_ot_lst+=[vol_ot]
            poi_vol_lst+=[poi_vol]
            mkt_vol_lst+=[mkt_vol]
            scen_lst+=[scen[ot_seg][i]]
#   result[result["scen_id"]==87] 
    share_out=pd.DataFrame({'poi':poi,
                           'scen_id':scen_id,
                           'share':share,
                           'num_ot':num_ot_lst,
                           'vol_ot':vol_ot_lst,
                           'poi_vol':poi_vol_lst,
                           'mkt_vol':mkt_vol_lst,
                           'scen':scen_lst})   
    
    share_out["city"]=cities[j]
    if j==0:
        result=share_out
    else:        
        result=pd.concat([result,share_out],ignore_index=True) 



result2=result.merge(pnl2,on=["city","poi"],how="left") \
              .merge(ims_shr2,how="left")
for ict in cities:
#    print ict
    print u"正在进行 %s factor的计算" % ict
    rlt=result2[result2["city"]==ict]
    for isc in rlt["scen_id"].drop_duplicates():
        rltsc=rlt[rlt["scen_id"]==isc].reset_index(drop=False).fillna(0)
        f= Variable()
        poi_ratio={}
        mkt_ratio={}
        
        for iprd in range(len(rltsc.index)): 
            poi_ratio[iprd]=np.divide((rltsc["poi_vol"][iprd]-rltsc["sales_pnl"][iprd])*f+rltsc["sales_pnl"][iprd],rltsc["ims_poi_vol"][iprd])-1
            mkt_ratio[iprd]=np.divide((rltsc["mkt_vol"][iprd]-rltsc["sales_pnl_mkt"][iprd])*f+rltsc["sales_pnl_mkt"][iprd],rltsc["ims_mkt_vol"][iprd])-1

        par=[]
        for s in range(len(rltsc.index)):
            if rltsc["poi"][s] in prd_input[:fst_prd]:
                par+= ["np.divide(abs(poi_ratio[%s])," % s+str(bias)+")"]
                par+= ["abs(mkt_ratio[%s])" % s]            
        exec ("obj=Minimize(max_elemwise("+",".join(par)+"))")  
#        obj=Minimize(max_elemwise(abs(poi_ratio[0]),abs(poi_ratio[1]),abs(poi_ratio[2]),abs(poi_ratio[3]), 
#                                  abs(mkt_ratio[0]),abs(mkt_ratio[1]),abs(mkt_ratio[2]),abs(mkt_ratio[3])))         
        prob = Problem(obj, [f>0])
        prob.solve()
        result2.loc[(result2["city"]==ict)&(result2["scen_id"]==isc),"factor"]=f.value
        for p,iprd in enumerate(rltsc["poi"]): 
            result2.loc[(result2["city"]==ict)&(result2["scen_id"]==isc)&(result2["poi"]==iprd),"poi_ratio"]=np.divide((rltsc["poi_vol"][p]-rltsc["sales_pnl"][p])*f.value+rltsc["sales_pnl"][p],rltsc["ims_poi_vol"][p])-1
            result2.loc[(result2["city"]==ict)&(result2["scen_id"]==isc)&(result2["poi"]==iprd),"mkt_ratio"]=np.divide((rltsc["mkt_vol"][p]-rltsc["sales_pnl_mkt"][p])*f.value+rltsc["sales_pnl_mkt"][p],rltsc["ims_mkt_vol"][p])-1
            result2.loc[(result2["city"]==ict)&(result2["scen_id"]==isc)&(result2["poi"]==iprd),"share_factorized"]=np.divide((rltsc["poi_vol"][p]-rltsc["sales_pnl"][p])*f.value+rltsc["sales_pnl"][p],(rltsc["mkt_vol"][p]-rltsc["sales_pnl_mkt"][p])*f.value+rltsc["sales_pnl_mkt"][p])
            result2.loc[(result2["city"]==ict)&(result2["scen_id"]==isc)&(result2["poi"]==iprd),"share_gap"]=np.divide((rltsc["poi_vol"][p]-rltsc["sales_pnl"][p])*f.value+rltsc["sales_pnl"][p],(rltsc["mkt_vol"][p]-rltsc["sales_pnl_mkt"][p])*f.value+rltsc["sales_pnl_mkt"][p])-rltsc[rltsc["poi"]==iprd]["ims_share"].tolist()[0]
    print u"已完成 %s factor的计算" % ict

print result2.shape

result2["rel_gap"]=np.divide(result2["share_gap"],result2["ims_share"])

#rlt_brf=result2[["city","scen","scen_id","num_ot","mkt_ratio","rel_gap","share_gap","poi_ratio","poi"]]
rlt_brf=result2[["city","ims_mkt_vol","scen","scen_id","num_ot","mkt_ratio","rel_gap","poi"]]
#result2[result2["scen_id"]==87]
#改参数
#collist=["city","scen_id","rel_gap","share_gap","poi_ratio"]
collist=["city","scen_id","rel_gap"]
#rlt_brf_out= \
#rlt_brf[rlt_brf["poi"]==prd_input[0]].merge(rlt_brf[rlt_brf["poi"]==prd_input[1]][collist],on=["city","scen_id"],suffixes=["_"+prd_input[0],""]) \
#                                     .merge(rlt_brf[rlt_brf["poi"]==prd_input[2]][collist],on=["city","scen_id"],suffixes=["_"+prd_input[1],""]) \
#                                     .merge(rlt_brf[rlt_brf["poi"]==prd_input[3]][collist],on=["city","scen_id"],suffixes=["_"+prd_input[2],"_"+prd_input[3]])



rlt_brf_out= \
rlt_brf[rlt_brf["poi"]==prd_input[0]].merge(rlt_brf[rlt_brf["poi"]==prd_input[1]][collist],on=["city","scen_id"]) \
                                     .merge(rlt_brf[rlt_brf["poi"]==prd_input[2]][collist],on=["city","scen_id"])
             
#改参数
writer=ExcelWriter(path+'/020EDA/'+u'Tide_ot_scen'+u"_凯纷_ot"+'.xlsx')
#writer=ExcelWriter(path+'/020EDA/'+u'EIA_ot_scen'+u"_AI_S"+'.xlsx')
result2 \
            .to_excel(writer, sheet_name=u'凯纷',encoding="GBK", index=False)
rlt_brf_out \
            .to_excel(writer, sheet_name='brf',encoding="GBK", index=False)            
writer.save()                 

#
#
##path9=u'C:/MAX/Bayer/2015/CV_Bay'
#writer=ExcelWriter(path+'/020EDA/'+u'QC.xlsx')
#seg_city \
#            .to_excel(writer, sheet_name='QC',encoding="GBK", index=False)
#writer.save()               
##
#pd.value_counts(result["city"])
#scen[ot_seg][50]
#ot_seg
#scen[ot_seg]
#len(scen[ot_seg])
#print cities[j]
#len(scen[ot_seg])
#
#data_EIA4[data_EIA4.loc[:,"City"] 
#                        .isin(cities)].groupby("City")["PANEL"].sum()
#                        
#result[(result["scen_id"]==95)&(result["city"].isin([u"烟台市",u"潍坊市",u"临沂市",u"济宁市"]))]
#result[(result["scen_id"]==19)&(result["city"].isin([u"扬州市",u"常州市",u"徐州市"]))]