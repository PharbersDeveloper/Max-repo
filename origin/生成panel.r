require(openxlsx)
require(dplyr)
require(tidyr)
require(stringi)
require(stringr)
require(feather)
require(RODBC)
require(MASS)
require(car)
require(tidyverse)
require(data.table)
library(reshape2)

Universe2019<-read.xlsx(paste0('L:/全原盘数据/D盘备份/',
                               'Pharbers文件/医院/20190218医院大全更新版/',
                               '2019年Universe更新维护1.0_190403.xlsx'),
                        sheet='Universe2019')

Universe20191<-Universe2019[,c('新版ID','新版名称')]%>%unique()%>%
  group_by(新版ID)%>%summarise(新版名称=first(新版名称))



mkt<-read.xlsx('E:/MAX/Tide/Global/通用名企业层面集中度_pb.xlsx')


map<-read.xlsx('E:/MAX/Tide/Global/泰德产品匹配表.xlsx')

c_month <- '1907'

raw<-fread(paste0('E:/MAX/Tide/UPDATE/',c_month,'/20',c_month,'补数结果.csv'),
           stringsAsFactors = F)

raw$商品名[is.na(raw$商品名)]<-raw$药品名称[is.na(raw$商品名)]
raw$min1<-paste(raw$商品名,raw$剂型,raw$规格,raw$包装数量,raw$生产企业,sep='|')

### 输出待清洗
need_cleaning <- raw[!raw$min1 %in% map$min1,
                     c('药品名称',"商品名","剂型","规格","包装数量",
                       "生产企业",'min1')] %>%unique()
if(nrow(need_cleaning)>0){
  write.xlsx(need_cleaning,
             paste0('E:/MAX/Tide/UPDATE/',substr(c_month,3,6),'/待清洗',Sys.Date(),'.xlsx'))
  winDialog('ok','已输出待清洗条目')
}

raw.v<-left_join(raw,
                 map[,c('min1','min2')],by='min1')%>%
  left_join(mkt[,c('市场','药品名称')]%>%unique(),by='药品名称')%>%
  left_join(Universe20191,by=c('新版ID'='新版ID'))


unique(raw.v$市场)

raw_pha2<-group_by(raw.v,ID=`医院编码`,Date,
                   Prod_Name=min2,DOI=市场,Hosp_name=`新版名称`,
                   HOSP_ID=`新版ID`,Prod_CNAME=min2,Strength=min2,
                   DOIE=市场)%>%
  summarise(Sales=sum(Sales),
            Units=sum(Units))
sum(raw_pha2$Sales)==sum(raw.v$Sales)
sum(raw.v$Sales[raw.v$add_flag%in%c(0,'0')])

year<-substr(raw_pha2$Date,1,4)%>%unique()

for (y in year) {
  for (mk in unique(raw_pha2$DOI)) {
    print(y)
    print(mk)
    tmp<- raw_pha2[substr(raw_pha2$Date,1,4)%in%y&
                     raw_pha2$DOIE%in%mk,]
    write.xlsx(tmp,
               paste0('E:/MAX/Tide/UPDATE/',c_month,'/',
                      mk,'_Panel_',c_month,'.xlsx'))
    
  }
  
}



