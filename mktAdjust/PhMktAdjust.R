
library("BPRSparkCalCommon")

cal_data_mkt_adjust <- function(raw_path, uni_2019_path, map_path, o_map_path, company_map_path) {
    raw <- read.df(raw_path, "parquet")
    raw <- mutate(raw,
                  商品名 = ifelse(isNull(raw$商品名), raw$药品名称, raw$商品名),
                 )
    
    raw <- CastCol2Double(raw, c("医院编码"))
    
    #### CPA 和 PHA 的匹配
    uni <- read.df(raw_path, "parquet")
    
    map <- read.df(map_path, "parquet")
    map <- select(filter(map, contains(map$CPA, c("0", "#N/A", NA, 0))),
                  c("新版ID", "CPA")
                 )
   
    map <- filter(map, !isNull(map$医院编码))
     
    #### CPA 和 PHA 的匹配 JNJ
    o_map <- read.df(o_map_path, "parquet")
    o_map <- select(filter(o_map, !isNull(o_map$CPA)),
                    c("CPA", "PHA_ID")
                   )
   
    suni <- select(uni, "新版ID", "PHA_ID")
    o_map <- join(o_map, suni, o_map$PHA_ID == suni$PHA_ID, "left")
   
    map_all <- distinct(union(map, select(o_map, "新版ID", "CPA")))
    map_all <- agg(groupBy(map_all, map_all$CPA),
                   新版ID = "first"
                  )
    map_all <- ColRename(map_all, c("CPA"), c("医院编码"))
    
    #### 专科信息
    uni <- select(filter(uni, !isNull(uni$`Re-Speialty`)), "新版ID", "Re-Speialty")
    uni <- agg(groupBy(uni, "新版ID"),
               `Re-Speialty` = "first"
              )
    
    raw_pha <- join(raw, map_all, raw$医院编码 == map_all$医院编码, "left")
    raw_pha <- join(raw_pha, uni, raw$新版ID == uni$新版ID, "left")
    
    #### 匹配企业类型，可更具pfc匹配
    company_map <- read.df(company_map_path, "parquet")
    raw_pha <- join(raw_pha, company_map, raw_pha$生产企业 == company_map$生产企业, "left")
    
    raw_pha_层面 <- agg(groupBy(raw_pha, "医院编码", "药品名称", "MNF_TYPE_STAND"),
                      金额 = "sum"
                       )
    # 这段没看懂
    # raw_pha2<-arrange(ungroup(raw_pha_层面),desc(药品名称),desc(MNF_TYPE_STAND),desc(`金额（元）`)) %>% 
    #     group_by(药品名称,MNF_TYPE_STAND) %>% mutate(累计百分比=cumsum(`金额（元）`/sum(`金额（元）`)))  %>% 
    #     mutate(rank=row_number())%>%summarise(n=max(rank),
    #                                           per50=min(rank[累计百分比>=0.5]),
    #                                           per75=min(rank[累计百分比>=0.75]),
    #                                           per90=min(rank[累计百分比>=0.9]),
    #                                           sales=sum(`金额（元）`))
    
    
    # c1<-raw_pha[(!is.na(raw_pha$新版ID)) & (!raw_pha$`Re-Speialty` %in% c(0,'0',NA)),]
    # c4<- group_by(c1,药品名称,MNF_TYPE_STAND,`Re-Speialty`) %>% 
    #     summarise(sales=sum(`金额（元）`)) %>% 
    #     arrange(desc(药品名称),MNF_TYPE_STAND,desc(sales)) %>% 
    #     mutate(rank=row_number()) %>% filter(rank<=3)%>%
    #     gather(re_sa,value,`Re-Speialty`:sales)%>%
    #     unite(re_sa_rank,c(re_sa,rank))%>%
    #     spread(re_sa_rank,`value`)
    # c4[,grepl('sales',names(c4))]<-sapply(c4[,grepl('sales',names(c4))],as.numeric)
    
    
    # raw_pha3<-left_join(raw_pha2,c4,by=c('药品名称','MNF_TYPE_STAND'))
    # raw_pha3$sales_1_share<-raw_pha3$sales_1/raw_pha3$sales
    # raw_pha3$sales_2_share<-raw_pha3$sales_2/raw_pha3$sales
    # raw_pha3$sales_3_share<-raw_pha3$sales_3/raw_pha3$sales
    
}