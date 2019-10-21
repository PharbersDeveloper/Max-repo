
library("BPRSparkCalCommon")

cal_data_mkt_adjust <- function(raw_path, uni_2019_path, map_path, o_map_path) {
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
    
}