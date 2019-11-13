
library("BPRSparkCalCommon")
cal_max_data_panel <- function(uni_2019_path, mkt_path, map_path, c_month, add_data) {
    uni <- read.df(uni_2019_path, "parquet")
    uni <- distinct(select(uni, "新版ID", "新版名称"))
   
    uni <- agg(groupBy(uni, "新版ID"), 
               新版名称=first("新版名称")
              )
    
    mkt <- read.df(mkt_path, "parquet")
    map <- read.df(map_path, "parquet")
    
    add_data <- mutate(
        add_data,
        商品名  = ifelse(!isNull(add_data$商品名),
                      add_data$商品名, add_data$std_mole)
    )
    coltypes(add_data)[which(names(add_data) %in% 
                                 c('包装数量'))] <- 'integer'
    add_data <- concat_multi_cols(add_data, c('商品名',
                                              '剂型',
                                              '规格',
                                              '包装数量',
                                              '生产企业'),
                                  'min1',
                                  sep = "|")
   
    persist(add_data, "MEMORY_ONLY")
    
    print(head(add_data))
    
    map1 <- distinct(select(map, 'min1'))
    ### 输出待清洗
    need_cleaning <- distinct(select(add_data %>%
                            join(map1, add_data$min1== map1$min1,'left_anti') %>%
                                drop_dup_cols(),
                            c("std_mole", "商品名", "剂型", "规格", 
                              "包装数量", "生产企业", "min1")
                           ))
    if(nrow(need_cleaning)>0){
        write.df(need_cleaning, "/Max-repo/need_cleaning", "parquet", "overwrite")
    }
    ### Panel
    mp <- distinct(select(map, "min1", "min2"))
    panel <- join(add_data, mp, add_data$min1 == mp$min1, "left") %>%
        drop_dup_cols()
    
    mkts <- distinct(select(mkt, "市场", "药品名称"))
    panel <- join(panel, mkts, panel$std_mole == mkts$药品名称, "left") %>%
        drop_dup_cols()
    
    panel <- join(panel, uni, panel$新版ID == uni$新版ID, "left") %>%
        drop_dup_cols()
    
    panel <- mutate(panel, Date = panel$Year * 100 + panel$Month)

    panel <- ColRename(
                agg(groupBy(panel, 
                     panel$医院编码,
                     panel$Date,
                     panel$min2,
                     panel$市场,
                     panel$新版名称,
                     panel$新版ID,
                     panel$std_mole,
                     panel$省,
                     panel$城市
                    ),
                 Sales = sum(panel$Sales),
                 Units = sum(panel$Units)),
                c("医院编码", "Date", "min2", "市场", 
                  "新版名称", "新版ID","std_mole","省","城市"),
                c("ID", "Date", "Prod_Name", "DOI", 
                  "Hosp_name", "HOSP_ID","Molecule", "Province","City"))
    
    panel <- mutate(panel, 
                    Prod_Name = panel$Prod_Name,
                    Strength = panel$Prod_Name,
                    DOIE = panel$DOI)
    
    panel <- partitionBy(panel, panel$Date, panel$DOI)
    
    write.df(panel, "/Map-repo/panel-result", "parquet", "overwrite")
}

concat_multi_cols <- function(df, cols, new_col, sep) {
    df[['tmp']] <- df[[cols[1]]]
    for (col in cols[-1]) {
        df <- mutate(df, tmp = concat(df[['tmp']], lit(sep), df[[col]]))
    }
    df <- withColumnRenamed(df, 'tmp', new_col)
    
    return(df)
}
