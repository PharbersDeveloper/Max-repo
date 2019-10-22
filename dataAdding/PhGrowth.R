universe <- read.xlsx('E:/MAX/尚杰/MODEL/000ref/universe.xlsx')
# 用同城市等级医院同药品（通用名）的年增长率,暂时不考虑2018年.这样会少一些补充医院
# 需不需要去掉多余的药品
growth.rate.raw <- total.rawdata %>% 
    left_join(unique(map1),
              by=c('新版ID'='新版ID'))
sum(is.na(growth.rate.raw$City.Tier.2010))
growth.rate.raw$City.Tier.2010[is.na(growth.rate.raw$City.Tier.2010)]<-5
unique(growth.rate.raw$新版ID[is.na(growth.rate.raw$City.Tier.2010)])
names(growth.rate.raw)[names(growth.rate.raw)=='City.Tier.2010']<-'CITYGROUP'
###
raw.id.with.pha <- unique(growth.rate.raw$新版ID)
growth.rate.raw$std_mole<-growth.rate.raw$药品名称


unique(growth.rate.raw$std_mole)

growth.rate <- growth.rate.raw %>%
    group_by(Year=年, std_mole, CITYGROUP) %>% 
    summarise(value=sum(`Sales`,na.rm = T)) %>% spread(Year, value, fill = 0) %>%
    mutate(GR1718=`2018`/`2017`)



# 检查销量
print(sum(growth.rate$`2018`+growth.rate$`2017`+growth.rate$`2019`)==
          sum(total.rawdata$Sales))

#增长率调整
growth.rate$GR1718[growth.rate$GR1718 %in% c(NA,Inf,0,NaN)] <- 1

growth.rate$GR1718[growth.rate$GR1718 < 0.1 |growth.rate$GR1718 > 10] <- 1

## 注意增长率异常的行
print(min(growth.rate$GR1718))
print(max(growth.rate$GR1718))

##输出原始数据ID跟城市等级的匹配
growth.rate.with.id <- 
    unique(growth.rate.raw[,c('新版ID','医院编码','City','CITYGROUP','std_mole')]) %>%
    left_join(growth.rate,by=c('CITYGROUP','std_mole'='std_mole'))
print(sum(is.na(growth.rate.with.id$GR1617)))
print(sum(is.na(growth.rate.with.id$GR1718)))

# new_hospital <- con.count.all1$新版ID[(!con.count.all1$`2018` %in% c(0,'0')) &
#                                      con.count.all1$`2017` %in% c(0,'0')] %>% unique()
new.hospital <- con.count.all1$新版ID[(!con.count.all1$`2018` %in% c(0,'0')) &
                                        con.count.all1$`2017` %in% c(0,'0')] %>% unique()

print(new.hospital)

raw.data.pha <- total.rawdata
setdiff(raw.data.pha$PHA.ID[raw.data.pha$PHA.ID %in% raw.id.with.pha &
                                !(raw.data.pha$Year %in% c('2019',2019))],growth.rate.with.id$ID)
####注意，这个增长率文件并不包含所有的原始ID，只包含历史年份文件在universe的ID。
####月度更新补数的时候匹不到的用城市再看看
write.xlsx(growth.rate.with.id,
           'E:/MAX/Tide/MODEL/凯时/000ref/各个分子分城市等级1718增长率文件.xlsx')



cal_growth <- function(raw_data, id_city){
    gr_raw_data <- join(raw_data, id_city,
                        raw_data$新版ID == id_city$新版ID, 'left') 
    colnames(gr_raw_data)[which(names(gr_raw_data) %in% c('新版ID'))[-1]] <- 
        'tmp'
    
    nrow(filter(gr_raw_data,isNull(gr_raw_data$City_Tier_2010)))
    
    
    
    gr_raw_data <- mutate(gr_raw_data, 
                          City_Tier_2010=ifelse(
                              isNull(gr_raw_data$City_Tier_2010),
                              5,
                              gr_raw_data$City_Tier_2010
                          ))
    
    gr_raw_data <- rename(gr_raw_data, CITYGROUP = gr_raw_data$City_Tier_2010,
                          std_mole = gr_raw_data$药品名称)
    
    # TODO: 翻译spread
    
        
}


