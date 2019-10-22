# 只为有pha的医院生成种子，因为只有这些医院能匹配上PHA城市等级.种子按照年份降序
seed <- total.rawdata %>% ungroup() %>% 
    filter(!is.na(新版ID))  %>% 
    arrange(desc(年)) %>%
    data.frame(stringsAsFactors = F)

seed$std_mole<-seed$药品名称


##17-18增长就用16-17增长代替
# growth_rate_with_id$GR1718 <- growth_rate_with_id$GR1617


seed.city.group <- seed %>%
    left_join(unique(map1),
              by = c('新版ID' = '新版ID')) %>%
    left_join(unique(growth.rate.with.id[,
                                         names(growth.rate.with.id) %in%
                                             c('CITYGROUP', 'std_mole') |
                                             grepl('GR1', names(growth.rate.with.id))]),
              by = c('City.Tier.2010'='CITYGROUP', 'std_mole'))%>%
    mutate(GR1819=GR1718)
names(seed.city.group)[names(seed.city.group)=='City.Tier.2010']<-'CITYGROUP'

sum(is.na(seed.city.group$CITYGROUP))


print(sum(is.na(seed.city.group$CITYGROUP)))
print(sum(is.na(seed.city.group$GR1718)))
print(sum(is.na(seed.city.group$GR1819)))

xixi <- seed.city.group[is.na(seed.city.group$GR1718),]
unique(xixi$Year)
###剩一些18年国药分子，暂时不理会

seed.city.group$GR1718[is.na(seed.city.group$GR1718)] <- 1
seed.city.group$GR1819[is.na(seed.city.group$GR1819)] <- 1

trans_raw_data_for_adding <- function(raw_data, id_city, gr_with_id){
    seed <- filter(raw_data, !isNull(raw_data$新版ID))
    seed <- arrange(seed, desc(seed$Year))
    seed <- join(seed, id_city, seed$新版ID==id_city$新版ID, 'left')
    
    # TODO:等上一个代码的含id的增长率
    seed <- join(seed, id_city, seed$新版ID==id_city$新版ID, 'left')
    
    seed <- rename(seed, CITYGROUP = seed$City_Tier_2010)
    
    seed <- mutate(seed, )
    
    
    return(seed)
    
}