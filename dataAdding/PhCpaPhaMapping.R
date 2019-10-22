
map_cpa_pha <- function(
    map_cpa_pha_path_v1,map_cpa_pha_path_v2,pha_id_transfer
    ) {
    
    map_cpa_pha_v1 <- read.df(map_cpa_pha_path_v1, "parquet")
    
    map_cpa_pha_v1 <- filter(
        map_cpa_pha_v1, !(map_cpa_pha_v1$CPA %in% c('0','#N/A',0,'NA')) & 
                              !isNull(map_cpa_pha_v1$CPA)
        )
    
    map_cpa_pha_v1 <- distinct(select(map_cpa_pha_v1, '新版ID','CPA'))
    
    print(head(map_cpa_pha_v1))
    
    
    map_cpa_pha_v2 <- read.df(map_cpa_pha_path_v2, "parquet")
    
    map_cpa_pha_v2 <- select(filter(
        map_cpa_pha_v2,!isNull(map_cpa_pha_v2$CPA)
        ), 'CPA', 'PHA_ID')
    
    origin_rows <- nrow(map_cpa_pha_v2)
    
    map_cpa_pha_v2 <- join(map_cpa_pha_v2, 
                           pha_id_transfer,
                           map_cpa_pha_v2$PHA_ID==pha_id_transfer$PHA_ID, 
                           'left')
    map_cpa_pha_v2 <- select(map_cpa_pha_v2, '新版ID', 'CPA')
    final_rows <- nrow(map_cpa_pha_v2)
    
    if(origin_rows!=final_rows){
        winDialog('ok','新老PHA匹配时出现一对多匹配')
    }
    
    
    map_cpa_pha <- rbind(map_cpa_pha_v1,map_cpa_pha_v2)

    map_cpa_pha <- agg(groupBy(map_cpa_pha, "CPA"), 
                       新版ID=first(map_cpa_pha$新版ID))
    
    coltypes(map_cpa_pha)[1] <- 'integer'
    
    map_cpa_pha <- filter(map_cpa_pha, !isNull(map_cpa_pha$CPA))
    
    return(map_cpa_pha)
}

