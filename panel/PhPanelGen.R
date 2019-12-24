
library("BPRSparkCalCommon")
cal_max_data_panel <- function(uni_path, mkt_path, map_path, c_month, add_data) {
    uni <- read_universe(uni_path)
    
    uni <- distinct(select(uni, "PHA", "HOSP_NAME"))
   
    uni <- agg(groupBy(uni, "PHA"), 
               HOSP_NAME=SparkR::first("HOSP_NAME")
              )
    
    mkt <- read.df(mkt_path, "parquet")
    map <- read.df(map_path, "parquet")
    
    full_product <- ifelse(!isNull(add_data$Brand),
                           add_data$Brand, add_data$Molecule)
    
    add_data <- mutate(
        add_data,
        Brand = full_product
    )
    coltypes(add_data)[which(names(add_data) %in% 
                                 c('包装数量'))] <- 'integer'
    add_data <- concat_multi_cols(add_data, c('Brand',
                                              'Specifications',
                                              'Manufacturer'),
                                  'min1',
                                  sep = "|")
   
    persist(add_data, "MEMORY_ONLY")
    
    print(head(add_data))
    
    map1 <- distinct(select(map, 'min1'))
    ### 输出待清洗
    need_cleaning <- distinct(select(add_data %>%
                            join(map1, add_data$min1== map1$min1,'left_anti') %>%
                                drop_dup_cols(),
                            c("Molecule", 'Brand',
                              'Specifications',
                              'Manufacturer', "min1")
                           ))
    if(nrow(need_cleaning)>0){
        need_cleaning_path = 
            paste0('Y:/MAX/Janssen/UPDATE/',c_month,'/待清洗',Sys.Date(),'.xlsx')
        print(paste("已输出待清洗文件至", need_cleaning_path))
        
        openxlsx::write.xlsx(collect(need_cleaning), 
                             need_cleaning_path)
    }
    ### Panel
    mp <- distinct(select(map, "min1", "min2", "通用名"))
    panel <- join(add_data, mp, add_data$min1 == mp$min1, "left") %>%
        drop_dup_cols()
    
    mkts <- distinct(select(mkt, "mkt", "通用名"))
    panel <- join(panel, mkts, panel$通用名 == mkts$通用名, "left") %>%
        drop_dup_cols()
    
    panel <- join(panel, uni, panel$PHA == uni$PHA, "left") %>%
        drop_dup_cols()
    
    panel <- mutate(panel, Date = panel$Year * 100 + panel$Month)

    panel <- ColRename(
                agg(groupBy(panel, 
                     panel$ID,
                     panel$Date,
                     panel$min2,
                     panel$mkt,
                     panel$HOSP_NAME,
                     panel$PHA,
                     panel$通用名,
                     panel$Province,
                     panel$City,
                     panel$add_flag
                    ),
                 Sales = sum(panel$Sales),
                 Units = sum(panel$Units)),
                c("ID", "Date", "min2", "mkt", 
                  "HOSP_NAME", "PHA","通用名","Province","City","add_flag"),
                c("ID", "Date", "Prod_Name", "DOI", 
                  "Hosp_name", "HOSP_ID","Molecule", "Province","City","add_flag"))
    
    panel <- mutate(panel, 
                    Prod_Name = panel$Prod_Name,
                    Strength = panel$Prod_Name,
                    DOIE = panel$DOI)
    
    #panel <- partitionBy(panel, panel$Date, panel$DOI)
    
    return(panel)
}

concat_multi_cols <- function(df, cols, new_col, sep) {
    df[['tmp']] <- df[[cols[1]]]
    for (col in cols[-1]) {
        df <- mutate(df, tmp = concat(df[['tmp']], lit(sep), df[[col]]))
    }
    df <- withColumnRenamed(df, 'tmp', new_col)
    
    return(df)
}
