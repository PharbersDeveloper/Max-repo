# require(openxlsx)
# require(dplyr)
# require(tidyr)
# require(stringi)
# require(stringr)
# require(feather)
# require(RODBC)
# require(MASS)
# require(car)
# require(tidyverse)
# require(data.table)
# library(reshape2)


# map<-read.xlsx(paste0('L:/全原盘数据/D盘备份/Pharbers文件/医院/',
#                       '20190218医院大全更新版/','2019年Universe更新维护1.0_190403.xlsx'),
#                sheet='Universe2019')
# map1<-map[map$重复==0,c('新版ID','City','City.Tier.2010')]%>%unique()

# map1$City[map1$新版ID%in%c('PHA0021658')]<-'大连市'
# map1$City[map1$City.Tier.2010%in%c('PHA0021658')]<-'3'


# ################################################整理匹配表
# map.cpa.pha<-read.xlsx(paste0('L:/全原盘数据/D盘备份/',
#                        'Pharbers文件/医院/20190218医院大全更新版/',
#                        '2019年Universe更新维护1.0_190403.xlsx'),
#                 sheet='Mapping')
# map.cpa.pha<-map.cpa.pha[!map.cpa.pha$CPA%in%c('0','#N/A',NA,0),c('新版ID','CPA')]%>%unique()
# #####CPA与PHA的匹配  JNJ
# map.cpa.pha.old<-read.xlsx(paste0('L:/全原盘数据/D盘备份/',
#                        'Pharbers文件/JNJ/hosp_potential/000ref/',
#                        'CPA_VS_GYC_VS_PHA_VS_HH_0418.xlsx'),
#                 sheet='Sheet1')
# map.cpa.pha.old.v<-map.cpa.pha.old[,c('CPA','PHA.ID')]%>%
#   filter(!is.na(CPA))%>%
#   left_join(map[,c('新版ID','PHA.ID')],
#             by=c('PHA.ID'='PHA.ID'))



# map_all<-map.cpa.pha[!is.na(map1.v$医院编码),c('新版ID','CPA')]%>%
#   rbind(map.cpa.pha.old.v[,c('新版ID','CPA')])%>%unique()%>%group_by(医院编码=CPA)%>%
#   summarise(新版ID=first(新版ID))


# ####################################################读取最原始数据

# cpa_gyc3<-read.xlsx('E:/MAX/Tide/Global/190815/190814泰德-1701-1906检索.xlsx',
#                     sheet='1701-1906数据')

# cpa_gyc3$医院编码<-as.numeric(cpa_gyc3$医院编码)

# total.rawdata<-cpa_gyc3%>%data.frame(stringsAsFactors = F)%>%
#   left_join(map_all,by=c('医院编码'='医院编码'))

# total.rawdata$Year<-total.rawdata$年%>%as.numeric()
# total.rawdata$Month<-total.rawdata$月%>%as.numeric()
# total.rawdata$Sales <- total.rawdata$`金额.元.`
# total.rawdata$Units <- total.rawdata$`数量.支.片.`

# raw.all <- total.rawdata

# names(raw.all)

# con <- unique(raw.all[,c('年','月','新版ID')]) # %>%
# #   filter(Year %in% c('2016',2016))

# con.count <- group_by(con,新版ID,年) %>%
#   count()


# con.count.wholeyear <- con.count  %>%
#   ungroup() %>% group_by(新版ID) %>%
#   summarise(MAX=max(n),
#             MIN=min(n))

# con.count.all <- left_join(con.count,con.count.wholeyear,by=c('新版ID'))


# con.count.all$MAX[is.na(con.count.all$MAX)] <- 0
# con.count.all$MIN[is.na(con.count.all$MIN)] <- 0

# distribution <- group_by(unique(con.count.all[,c('MAX','MIN','新版ID')]),MAX,MIN) %>% count()


# con.count.all1<-con.count.all[,1:3]%>%unique()%>%
#   spread(年,n,fill=0)%>%
#   mutate(`总出现次数`=`2017`+`2018`+`2019`)%>%
#   mutate(min=pmin(`2017`,`2018`,`2019`),
#          max=pmax(`2017`,`2018`,`2019`))
# write.xlsx(con.count.all1,
#            'E:/MAX/Tide/Global/Tide项目医院连续性表现_医院层面.xlsx')
# write.xlsx(con.count.all,
#            'E:/MAX/Tide/Global/医院两年半连续性表现.xlsx')


# ######开始补数

# universe <- read.xlsx('E:/MAX/尚杰/MODEL/000ref/universe.xlsx')
# # 用同城市等级医院同药品（通用名）的年增长率,暂时不考虑2018年.这样会少一些补充医院
# # 需不需要去掉多余的药品
# growth.rate.raw <- total.rawdata %>% 
#   left_join(unique(map1),
#             by=c('新版ID'='新版ID'))
# sum(is.na(growth.rate.raw$City.Tier.2010))
# growth.rate.raw$City.Tier.2010[is.na(growth.rate.raw$City.Tier.2010)]<-5
# unique(growth.rate.raw$新版ID[is.na(growth.rate.raw$City.Tier.2010)])
# names(growth.rate.raw)[names(growth.rate.raw)=='City.Tier.2010']<-'CITYGROUP'
# ###
# raw.id.with.pha <- unique(growth.rate.raw$新版ID)
# growth.rate.raw$std_mole<-growth.rate.raw$药品名称


# unique(growth.rate.raw$std_mole)

# growth.rate <- growth.rate.raw %>%
#   group_by(Year=年, std_mole, CITYGROUP) %>% 
#   summarise(value=sum(`Sales`,na.rm = T)) %>% spread(Year, value, fill = 0) %>%
#   mutate(GR1718=`2018`/`2017`)



# # 检查销量
# print(sum(growth.rate$`2018`+growth.rate$`2017`+growth.rate$`2019`)==
#         sum(total.rawdata$Sales))

# #增长率调整
# growth.rate$GR1718[growth.rate$GR1718 %in% c(NA,Inf,0,NaN)] <- 1

# growth.rate$GR1718[growth.rate$GR1718 < 0.1 |growth.rate$GR1718 > 10] <- 1

# ## 注意增长率异常的行
# print(min(growth.rate$GR1718))
# print(max(growth.rate$GR1718))

# ##输出原始数据ID跟城市等级的匹配
# growth.rate.with.id <- 
#   unique(growth.rate.raw[,c('新版ID','医院编码','City','CITYGROUP','std_mole')]) %>%
#   left_join(growth.rate,by=c('CITYGROUP','std_mole'='std_mole'))
# print(sum(is.na(growth.rate.with.id$GR1617)))
# print(sum(is.na(growth.rate.with.id$GR1718)))

# # new_hospital <- con.count.all1$新版ID[(!con.count.all1$`2018` %in% c(0,'0')) &
# #                                      con.count.all1$`2017` %in% c(0,'0')] %>% unique()
# new.hospital <- con.count.all1$新版ID[(!con.count.all1$`2018` %in% c(0,'0')) &
# con.count.all1$`2017` %in% c(0,'0')] %>% unique()

# print(new.hospital)

# raw.data.pha <- total.rawdata
# setdiff(raw.data.pha$PHA.ID[raw.data.pha$PHA.ID %in% raw.id.with.pha &
#                               !(raw.data.pha$Year %in% c('2019',2019))],growth.rate.with.id$ID)
# ####注意，这个增长率文件并不包含所有的原始ID，只包含历史年份文件在universe的ID。
# ####月度更新补数的时候匹不到的用城市再看看
# write.xlsx(growth.rate.with.id,
#            'E:/MAX/Tide/MODEL/凯时/000ref/各个分子分城市等级1718增长率文件.xlsx')

# # 先生成种子文件,用最近年份（不一定相邻）且优先大年份的数据补


# # 只为有pha的医院生成种子，因为只有这些医院能匹配上PHA城市等级.种子按照年份降序
# seed <- total.rawdata %>% ungroup() %>% 
#   filter(!is.na(新版ID))  %>% 
#   arrange(desc(年)) %>%
#   data.frame(stringsAsFactors = F)

# seed$std_mole<-seed$药品名称


# ##17-18增长就用16-17增长代替
# # growth_rate_with_id$GR1718 <- growth_rate_with_id$GR1617


# seed.city.group <- seed %>%
#   left_join(unique(map1),
#             by = c('新版ID' = '新版ID')) %>%
#   left_join(unique(growth.rate.with.id[,
#                                        names(growth.rate.with.id) %in%
#                                          c('CITYGROUP', 'std_mole') |
#                                          grepl('GR1', names(growth.rate.with.id))]),
#             by = c('City.Tier.2010'='CITYGROUP', 'std_mole'))%>%
#   mutate(GR1819=GR1718)
# names(seed.city.group)[names(seed.city.group)=='City.Tier.2010']<-'CITYGROUP'

# sum(is.na(seed.city.group$CITYGROUP))


# print(sum(is.na(seed.city.group$CITYGROUP)))
# print(sum(is.na(seed.city.group$GR1718)))
# print(sum(is.na(seed.city.group$GR1819)))

# xixi <- seed.city.group[is.na(seed.city.group$GR1718),]
# unique(xixi$Year)
# ###剩一些18年国药分子，暂时不理会

# seed.city.group$GR1718[is.na(seed.city.group$GR1718)] <- 1
# seed.city.group$GR1819[is.na(seed.city.group$GR1819)] <- 1




# seed.all.years <- NULL

# ##按年生成种子
# seed.city.group$PHA.ID<-seed.city.group$新版ID
# for(y in unique(seed.city.group$Year)){
#   print(y)
#   current_data <- seed.city.group[seed.city.group$Year == y,]
  
#   current_range <- paste(current_data$PHA.ID, current_data$Month) %>% unique()
#   #用其他Year份生成当YearYear份的种子
#   ##新Year份时间差减去0.5使得新Year份会比老Year份更相邻
#   seed_other_years <- seed.city.group[seed.city.group$Year != y,] %>% 
#     filter(Month %in% current_data$Month[current_data$Year==y]) %>%
#     filter(!(paste(PHA.ID, Month) %in% current_range)) %>%
#     mutate(time_diff = as.numeric(Year) - as.numeric(y),
#            weight = ifelse(as.numeric(Year) > as.numeric(y),
#                            as.numeric(Year) - as.numeric(y) - 0.5,
#                            as.numeric(y) - as.numeric(Year)))
#   seed_range <- seed_other_years %>% arrange(weight) %>%
#     group_by(PHA.ID = PHA.ID, Month = Month) %>%
#     summarise(Year = first(Year)) %>% mutate(range = paste(PHA.ID, Year, Month))
  
#   seed_tmp <- seed_other_years %>%
#     filter(paste(PHA.ID,Year,Month) %in% seed_range$range)
  
#   rate_index <- which(substr(names(seed_tmp),3,4)==substr(y,3,4))
#   if(length(rate_index)==0){
#     rate_index <- which(substr(names(seed_tmp),3,4)==substr(as.numeric(y)-1,3,4))+1
#   }
  
#   new_sales <- sapply(1:nrow(seed_tmp), function(x){
#     if(seed_tmp[x,'time_diff']>0){
#       index_range <- rate_index:(seed_tmp[x,'time_diff']-1+rate_index)
#     }else(
#       index_range <- (seed_tmp[x,'time_diff']+rate_index):(rate_index-1)
#     )
#     print(x)
#     print(names(seed_tmp)[index_range])
#     power <- (-1) * sign(seed_tmp[x,'time_diff'])
    
#     value <- prod(seed_tmp[x,index_range])^power * seed_tmp[x,'Sales']
#     return(value)
#   })
#   new_units <- sapply(1:nrow(seed_tmp), function(x){
#     if(seed_tmp[x,'time_diff']>0){
#       index_range <- rate_index:(seed_tmp[x,'time_diff']-1+rate_index)
#     }else(
#       index_range <- (seed_tmp[x,'time_diff']+rate_index):(rate_index-1)
#     )
#     # print(x)
#     # print(names(seed_tmp)[index_range])
#     power <- (-1) * sign(seed_tmp[x,'time_diff'])
    
#     value <- prod(seed_tmp[x,index_range])^power * seed_tmp[x,'Units']
#     return(value)
#   })
#   seed_tmp$Sales <- new_sales
#   seed_tmp$Units <- new_units
#   seed_tmp$Year <- y
#   seed.all.years <- rbind(seed.all.years, seed_tmp)
# }





# ##检查扩充的种子文件

# seed.time.limited <- 
#   seed.all.years[paste(seed.all.years$Year,
#                        seed.all.years$Month) %in%
#                    paste(total.rawdata$Year,total.rawdata$Month),]


# print(nrow(seed.time.limited)==nrow(seed.all.years))



# raw.sales.by.y <-
#   group_by(total.rawdata, Year) %>%
#   summarise(value = sum(Sales,na.rm=T),
#             Units = sum(Units,na.rm=T))




# # 合并种子和原始数据
# seed.drop.duplicate <-
#   seed.time.limited[!(
#     paste(
#       seed.time.limited$PHA.ID,
#       seed.time.limited$Year,
#       seed.time.limited$Month
#     ) %in%
#       paste(total.rawdata$PHA.ID, total.rawdata$Year, total.rawdata$Month)
#   ), ]

# print(nrow(seed.drop.duplicate)==nrow(seed.all.years))
# total.rawdata$PHA.ID<-total.rawdata$新版ID
# raw.data.add <- rbind(merge(total.rawdata, 0),
#                       merge(seed.drop.duplicate[,
#                                                 names(total.rawdata)], 1))
# names(raw.data.add)[names(raw.data.add)=='y'] <- 'add_flag'



#  #补充
#  months <- length(unique(total.rawdata$Month[total.rawdata$Year=='2019']))
#  add.months <- setdiff(1:12,1:months)

#  #按照除月份、金额数量之外的列group
#  raw.data.add.new.hosp <-
#    group_by_(raw.data.add[raw.data.add$新版ID %in% new.hospital,],
#              .dots=lapply(names(raw.data.add)[c(1:3,6:12,15:19,23)],as.symbol)) %>%
#    summarise(Sales=sum(Sales)/months,
#              Units=sum(Units)/months) %>% merge(add.months) %>%
#    filter(paste(Year,y)%in% paste(total.rawdata$Year,total.rawdata$Month))

#  #两个比值需要相等
#  print(sum(raw.data.add$Sales[raw.data.add$新版ID %in% new.hospital &
#                                 raw.data.add$Year != '2019'])/
#          sum(raw.data.add.new.hosp$Sales[raw.data.add.new.hosp$Year != '2019']))
#  print(months/(length(add.months)))


#  names(raw.data.add.new.hosp)[names(raw.data.add.new.hosp) %in% c('y','y[FALSE, ]')] <- 'Month'
#  raw.data.add.new.hosp$季度 <- floor((raw.data.add.new.hosp$Month +2)/3)
#  if(nrow(raw.data.add.new.hosp)==0){
#    raw.data.add.new.hosp$add_flag <- numeric(0)
#  }else{
#    raw.data.add.new.hosp$add_flag <- 1
#  }



#  #中间列去掉
# var<-intersect(names(raw.data.add),names(raw.data.add.new.hosp))
 
 
#  raw.data.add.new.hosp.m <-
#    raw.data.add.new.hosp[,var]

#  raw.data.complete <-
#    rbind(raw.data.add[,var],
#          raw.data.add.new.hosp.m)


# names(raw.data.add)
# # names(raw.data.add.new.hosp)
# # setdiff(names(raw.data.add.new.hosp),names(raw.data.add))
# # setdiff(names(raw.data.add),names(raw.data.add.new.hosp))

# raw.final.sales.by.y <- 
#   group_by(raw.data.complete, Year) %>%
#   summarise(value=sum(Sales),
#             Units=sum(Units))

# ##既有PHAID又满足连续性   老版代码是将所有新增医院当样本，现在需要对新增医院在新年份上的连续性做判断，只留连续性好的
# #  con.count.new <- con.count %>% filter(年 %in% '2019' & 新版ID %in% new.hospital)
# #  new.con.hosp <- con.count.new$ID[con.count.new$n>=6]
# #  
# #  
# #  sample.id <- con.count.all$新版ID[((con.count.all$新版ID %in% new.con.hosp) | (con.count.all$MAX > 9)) &
# #                                  (con.count.all$新版ID %in% 
# #                                     total.rawdata$新版ID[!is.na(total.rawdata$HOSP_ID) &
# #                                                        total.rawdata$HOSP_ID %in% universe$Panel_ID])] %>%
# #    unique()
# #  sample.pha.id <- total.rawdata[total.rawdata$ID %in% sample.id,c('ID','HOSP_ID')] %>%
# #    unique()
# #  write.xlsx(sample.pha.id,'E:/MAX/Amgen/Global/医院匹配_样本编码.xlsx')
# #  检查是否所有sample医院都每年出现月份数大于9
# #  continuity.confirm <- unique(raw.data.complete[raw.data.complete$ID %in% 
# #                                                   sample.id,
# #                                                 c('ID','Year','Month')]) %>%
# #    group_by(ID,Year) %>% mutate(n=max(row_number()))
# # 
# # 应该只有2018,少数几家别的年份的,即使补充了也不够
# #  print(unique(continuity.confirm$Year[continuity.confirm$n<10]))
# #  unique(continuity.confirm$ID[continuity.confirm$n<10 & continuity.confirm$Year != 2018])
# #  unique(sample.pha.id$HOSP_ID[sample.pha.id$ID %in% 
# #                                 unique(continuity.confirm$ID[continuity.confirm$n<10 & 
# #                                                                continuity.confirm$Year != 2018])])
# # 
# #  为补充医院和segment都增添标记
# # 
# #  add.hospital <- unique(seed.drop.duplicate$PHA.ID)
# #  raw.data.add.flag <- raw.data.complete %>%
# #    left_join(universe[,c('Panel_ID','Seg','CITYGROUP')],by=c('HOSP_ID'='Panel_ID'))
# #  
# #  print(sum(is.na(raw.data.add.flag$Seg[raw.data.add.flag$HOSP_ID %in% raw.id.with.pha])))
# #  
# #  raw.data.add.flag$add_flag_hospital <- 
# #    raw.data.add.flag$ID %in%
# #    add.hospital
# #  
# #  
# #  add.segment <- unique(raw.data.add.flag$Seg[raw.data.add.flag$add_flag_hospital])
# #  
# #  raw.data.add.flag$add_flag_segment <- 
# #    raw.data.add.flag$Seg %in%
# #    add.segment
# #  
# #  
# #  sum(raw.data.add.flag$Sales)

# ##补数前后的原始部分两个值要相等
# sum(total.rawdata$Sales)
# sum(raw.data.complete$Sales[raw.data.complete$add_flag==0])

# #占比

# sum(raw.data.complete$Sales[raw.data.complete$add_flag==1])/
#   sum(raw.data.complete$Sales[raw.data.complete$add_flag==0])

# #这是样本部分的扩大比例，毕竟只有样本部分补过数
# # sum(raw.data.add.flag$Sales[raw.data.add.flag$add_flag==1])/
# #   sum(raw.data.add.flag$Sales[raw.data.add.flag$add_flag==0 & raw.data.add.flag$ID %in% sample.id])

# # sum(raw.data.add.flag$Units)

# ##补数前后的原始部分两个值要相等
# sum(total.rawdata$Units)
# sum(raw.data.complete$Units[raw.data.complete$add_flag==0])
# ##输出。在spotfire里检查补充是否合理。###Date尚未替换

# ##这个文件还能用来补月度刷新的缺失医院

# # counti<-read.xlsx('E:/MAX/锦州奥鸿/Global/样本连续性.xlsx')
# # qc<-left_join(raw.data.complete,counti,by=c('PHA.ID'='PHA.ID'))
# # sum(qc$Sales[qc$n>=6&!is.na(qc$PHA.ID)],na.rm=T)/sum(qc$Sales[!is.na(qc$PHA.ID)],na.rm=T)
# # sum(qc$Sales[qc$n>=6&!is.na(qc$PHA.ID)&
# #                qc$add_flag==0&qc$PHA.ID%in%universe$Panel_ID],na.rm=T)/sum(qc$Sales[!is.na(qc$PHA.ID)&
# #                                                                                       qc$add_flag==0],na.rm=T)

# raw.data.complete$Date<-as.numeric(raw.data.complete$Year)*100+as.numeric(raw.data.complete$Month)
# raw.data.complete$季度<-floor((raw.data.complete$Date +2)/3)

# write.csv(raw.data.complete,
#           'E:/MAX/Tide/UPDATE/201701-201906补数结果.csv',
#           row.names = F)


# #########
# coun<-raw.data.complete[as.numeric(raw.data.complete$Date)>=201801&
#                           as.numeric(raw.data.complete$Date)<=201812 ,
#                         c('PHA.ID','Date')]%>%unique()%>%group_by(PHA.ID)%>%count()
