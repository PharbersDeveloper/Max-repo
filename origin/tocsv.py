# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""



import codecs
import pandas as pd
from pandas import DataFrame,Series,ExcelWriter


path = u'D:/Pharbers文件/Pfizer/MAX/'+mkt+'/050forPIC/'

path = u'D:/Pharbers文件/Pfizer/MAX/'+'HTN'+'/050forPIC/'

path=u'H:/Pfizer_txt_ZBY/'
path=u'L:/MAX/Qilu/MODEL/TXT/1810/'
path=u'I:/bayer_txt/'
path=u'I:/UCB_txt/'
path=u'I:/pfizer_txt20170310/'
path=u'L:/MAX result/Astellas/1810/'
path=u'F:/lr/'
path=u'D:/Pharbers文件/Lilly/MAX/dpp4/050forPIC/'
path=u'L:/MAX result/NHWA/TXT/'
path=u'I:/LJX/bayer提数/Bayer新universe/'
path=u'I:/Novartis/'
path=u'E:/MAX/Servier/UPDATE/txt/201905/'
path=u'L:/MAX result/XLT/TXT/'
path=u'G:/Pharbers/信立泰/MAX结果文件/'
path=u'G:/Pharbers/Astellas/MAX/前列腺癌/040max_output/'
path=u'H:/LJX/罗然零食文件夹/NHWA/'
path=u'G:/Bayer_txt/'
path=u'H:/qlwang/提数/新建文件夹/'
path=u'J:/MAX/BMS/Model/Chemo/040max_output/'
path=u'L:/MAX result/Beite/1601-1809/'
path=u'L:/MAX result/BMS/1810/'
path=u'L:/MAX result/Beite/1902/'
path=u'L:/MAX result/Bayer/1812/'
path=u'E:/MAX/Servier/UPDATE/txt/201902/'
path=u'L:/MAX result/zhengdatianqing/TXT/1901/'
path=u'L:/MAX result/Lilly/1902/'
path=u'L:/pfizer/辉瑞TXT/新增分子/'
path=u'L:/MAX result/Bayer/17/'
path=u'L:/MAX result/Amgen/1904/'
path=u'E:/广阔市场/蓝皮书项目/县医院/Model/HTN/040max_output/'
path=u'L:/MAX result/Sankyo/1904/'
path=u'L:/MAX result/jinzhouaohong/1903/'

fn=u'HTN_Factorized_Units&Sales_WITH_OT'
fn=u'MAX_' + mkt + '_20000_wo_ot_1' + y
fn=u'MAX_' + mkt + '_phz_hy2'
fn=u'MAX_' + mkt + '_p2_20000_wo_ot_1' + y
fn=u'MAX_' + mkt + '_wt2_20000_wo_ot_1' + y
fn=u'MAX_' + mkt + '_20000_wo_ot_1' + y+'_part2'
fn=u'MAX_' + mkt + '_20000_wo_ot_1' + y+'_part3'
fn=u'MAX_' + mkt + '_p5_20000_wo_ot_1' + y
fn=u'MAX_' + mkt + '_p6_20000_wo_ot_1' + y
fn=u'lizhu15onc'


# 'AT','AHP','HC','IVAB','NIAD','ONC','OSAB','PAI','STH','HRT'
#'SLX', 'ART', 'FRT', 'MYO','MINO', 'Glakay', 'HMI', 'MRS', 'ELD'
#'AT','AHP','HCC','IVAB','OAD','WH','OSAB','PAI','STH','RCC'
#'RP','SA'
'CNS_R','CNS_Z','Urology_viagra','Urology_other','Specialty_champix','Specialty_other',
    'INF','AI_S','PAIN_C','ZYVOX','ELIQUIS','PAIN_lyrica','PAIN_other','AI_D', 'AI_W','AI_R_zith',
    'AI_R_other','LD','HTN2','ONC_aml','ONC_other','HTN'
#    'Allelock','Grafalon','Mycamine','Gout','Perdipine','Prograf','Vesicare',"Harnal",u"前列腺癌"
#'CNS_R','CNS_Z','Urology','Specialty','INF','AI_S','PAIN_C','ZYVOX','ELIQUIS','PAIN','AI_D', 'AI_W','AI_R','LD','HTN2','ONC','HTN'
#'RAASI PLAIN','RAASI FDC','OAD','Dyslipidemia','IHD'
 #'Beite'   
#'Basal Analog','Human Insulin','Mealtime Analog'
 # 'OLM','MVT','LOX Tab','CV IV'
for y in ['905']:
    for mkt in [u'Dyslipidemia']:
#        path = u'D:/Pharbers文件/Pfizer/MAX/'+mkt+'/050forPIC/'
        fn=u'MAX_' + mkt +'_20000_wo_ot_1' + y
        doc = codecs.open(path+fn+'.txt','rU','UTF-16')
        txt_data=pd.read_table(doc,sep='\t',header=0)
        print(txt_data.shape)
        print(mkt)
        txt_data.to_csv(path + fn + \
                    '.csv',index=False,encoding='gbk')
        doc.close()
    
doc = codecs.open(u'I:/LJX/bayer提数/Bayer新universe/sanofi/sanofi161709'+'.txt','rU','UTF-16')
txt_data=pd.read_table(doc,sep='\t',header=0)  
txt_data.to_csv(u'I:/LJX/bayer提数/Bayer新universe/sanofi/sanofi161709' + \
                    '.csv',index=False,encoding='gbk')
doc.close()

doc1 = codecs.open(u'I:\LJX\bayer提数\Bayer新universe/sanofi/'+u'浙江_prod模型预测结果'+'.txt','rU','UTF-16')
txt_data1=pd.read_table(doc1,sep='\t',header=0)  
txt_data1.to_csv(u'D:/Pharbers文件/AZ CHC/ljx/' + u'浙江_prod模型预测结果' + \
                    '.csv',index=False,encoding='utf8')
doc.close()
        

for y in ['3']:
    for mkt in ['AI_R']:
        txt_data1 = pd.DataFrame()
        for i in ['1','2','3','4','5','6']:
            fn=u'MAX_' + mkt + '_p'+i+'_20000_wo_ot_1' + y
            doc = codecs.open(path+fn+'.txt','rU','UTF-16')
            txt_data=pd.read_table(doc,sep='\t',header=0)
            txt_data.rename(columns={u'sum(Predict_Sales)':txt_data02.columns[-2], u'sum(Predict_Units)':u'(2)'}, inplace = True)
            txt_data1=txt_data1.append(txt_data)
            doc.close()
        print(txt_data1.shape)
        print(txt_data1.columns)
        fn=u'MAX_' + mkt + '_20000_wo_ot_1' + y
        txt_data1.to_csv(path + fn + \
                    '.csv',index=False,encoding='gbk')
        
for y in ['3']:
    for mkt in ['LD']:
        txt_data1 = pd.DataFrame()
        for i in ['1','2']:
            fn=u'MAX_' + mkt + '_p'+i+'_20000_wo_ot_1' + y
            doc = codecs.open(path+fn+'.txt','rU','UTF-16')
            txt_data=pd.read_table(doc,sep='\t',header=0)
            txt_data1=txt_data1.append(txt_data)
            doc.close()
        print(txt_data1.shape)
        print(txt_data1.columns)
        fn=u'MAX_' + mkt + '_20000_wo_ot_1' + y
        txt_data1.to_csv(path + fn + \
                    '.csv',index=False,encoding='gbk')

for y in [u'boliwei_3_新版spotfire_取整']:
    for mkt in ['xinlitai_']:
#        path = u'D:/Pharbers文件/Pfizer/MAX/'+mkt+'/050forPIC/'
        fn=mkt + y
        doc = codecs.open(path+fn+'.txt','rU','UTF-16')
        txt_data=pd.read_table(doc,sep='\t',header=0)
        print(txt_data.shape)
        print(mkt)
        txt_data.to_csv(path + fn + \
                    '.csv',index=False,encoding='gbk')
        doc.close()
    

#txt_data=pd.read_csv(path+fn+'.csv',encoding="gbk")
#txt_data2=pd.read_csv(path+fn+'.csv',encoding="gbk")
#fn1=u'MAX_' + mkt + '_wt_20000_wo_ot_1' + y


txt_data2=pd.read_table(doc,sep='\t',header=0)
txt_data3=pd.read_table(doc,sep='\t',header=0)
txt_data4=pd.read_table(doc,sep='\t',header=0)
txt_data5=pd.read_table(doc,sep='\t',header=0)
txt_data6=pd.read_table(doc,sep='\t',header=0)

txt_data7=txt_data.append(txt_data2).append(txt_data3).append(txt_data4).append(txt_data5).append(txt_data6)