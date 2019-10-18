require(openxlsx)
require(dplyr)
require(tidyr)
require(sca)
require(readr)
require(stringr)
require(stringi)
require(data.table)
require(RODBC)
require(sqldf)
require(feather)

getSheetNames('E:/MAX/Tide/Global/190815/190814泰德-1701-1906检索.xlsx')

raw<-read.xlsx('E:/MAX/Tide/Global/190815/190814泰德-1701-1906检索.xlsx',
               sheet='1701-1906数据')

raw$`商品名`[is.na(raw$`商品名`)]<-raw$`药品名称`[is.na(raw$`商品名`)]
raw$医院编码<-as.numeric(raw$医院编码)

#####CPA与PHA的匹配
Universe2019<-read.xlsx(paste0('L:/全原盘数据/D盘备份/',
                               'Pharbers文件/医院/20190218医院大全更新版/',
                               '2019年Universe更新维护1.0_190403.xlsx'),
                        sheet='Universe2019')


map1<-read.xlsx(paste0('L:/全原盘数据/D盘备份/',
                       'Pharbers文件/医院/20190218医院大全更新版/',
                       '2019年Universe更新维护1.0_190403.xlsx'),
                sheet='Mapping')
map1.v<-map1[!map1$CPA%in%c('0','#N/A',NA,0),c('新版ID','CPA')]%>%unique()
#####CPA与PHA的匹配  JNJ
map2<-read.xlsx(paste0('L:/全原盘数据/D盘备份/',
                       'Pharbers文件/JNJ/hosp_potential/000ref/',
                       'CPA_VS_GYC_VS_PHA_VS_HH_0418.xlsx'),
                sheet='Sheet1')
map2.v<-map2%>%filter(!is.na(CPA))%>%select(CPA,PHA.ID)%>%
  left_join(Universe2019[,c('新版ID','PHA.ID')],by=c('PHA.ID'='PHA.ID'))
#names(map2.v)[names(map2.v)=='CPA']<-'医院编码'
map_all<-map1.v[!is.na(map1.v$医院编码),c('新版ID','CPA')]%>%
  rbind(map2.v[,c('新版ID','CPA')])%>%unique()%>%group_by(医院编码=CPA)%>%
  summarise(新版ID=first(新版ID))

#############专科信息
Universe2019.v<-Universe2019[!is.na(Universe2019$`Re-Speialty`),
                            c('新版ID','Re-Speialty')]%>%
  group_by(新版ID)%>%
  summarise(`Re-Speialty`=first(`Re-Speialty`))



raw_pha<-raw%>%left_join(map_all,by=c('医院编码'='医院编码'))%>%
  left_join(Universe2019.v,
            by=c('新版ID'='新版ID'))

sum(raw_pha$金额[!is.na(raw_pha$新版ID)])/sum(raw_pha$金额)

##############################匹配企业类型，可根据pfc匹配
company_map<-read.xlsx("E:/MAX/Tide/Global/company.global.xlsx")
raw_pha<-raw_pha%>%left_join(company_map,
  by=c('生产企业'='生产企业'))

raw_pha_层面<-group_by(raw_pha,医院编码,药品名称,MNF_TYPE_STAND)%>%
  summarise(`金额（元）`=sum(`金额（元）`))

raw_pha2<-arrange(ungroup(raw_pha_层面),desc(药品名称),desc(MNF_TYPE_STAND),desc(`金额（元）`)) %>% 
  group_by(药品名称,MNF_TYPE_STAND) %>% mutate(累计百分比=cumsum(`金额（元）`/sum(`金额（元）`)))  %>% 
  mutate(rank=row_number())%>%summarise(n=max(rank),
                                        per50=min(rank[累计百分比>=0.5]),
                                        per75=min(rank[累计百分比>=0.75]),
                                        per90=min(rank[累计百分比>=0.9]),
                                        sales=sum(`金额（元）`))


c1<-raw_pha[(!is.na(raw_pha$新版ID)) & (!raw_pha$`Re-Speialty` %in% c(0,'0',NA)),]
c4<- group_by(c1,药品名称,MNF_TYPE_STAND,`Re-Speialty`) %>% 
  summarise(sales=sum(`金额（元）`)) %>% 
  arrange(desc(药品名称),MNF_TYPE_STAND,desc(sales)) %>% 
  mutate(rank=row_number()) %>% filter(rank<=3)%>%
  gather(re_sa,value,`Re-Speialty`:sales)%>%
  unite(re_sa_rank,c(re_sa,rank))%>%
  spread(re_sa_rank,`value`)
c4[,grepl('sales',names(c4))]<-sapply(c4[,grepl('sales',names(c4))],as.numeric)


raw_pha3<-left_join(raw_pha2,c4,by=c('药品名称','MNF_TYPE_STAND'))
raw_pha3$sales_1_share<-raw_pha3$sales_1/raw_pha3$sales
raw_pha3$sales_2_share<-raw_pha3$sales_2/raw_pha3$sales
raw_pha3$sales_3_share<-raw_pha3$sales_3/raw_pha3$sales

write.xlsx(raw_pha3,'E:/MAX/Tide/Global/通用名企业层面集中度.xlsx')

