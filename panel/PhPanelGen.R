
cal_max_data_panel <- function(uni_2019_path, mkt_path, map_path, c_month, add_data) {
    uni <- read.df(uni_2019_path, "parquet")
    uni <- distinct(select(uni, "新版ID", "新版名称"))
   
    uni <- agg(groupBy(uni, "新版ID"), 
               新版名称=first("新版名称")
              )
    
    mkt <- read.df(mkt_path, "parquet")
    map <- read.df(map_path, "parquet")
    
    add_data <- mutate(add_data,
                       商品名 = ifelse(is.na(add_data$商品名), add_data$商品名, add_data$药品名称),
                       min1 = paste(add_data$商品名, 
                                    add_data$剂型, 
                                    add_data$规格, 
                                    add_data$包装数量,
                                    add_data$生产企业, 
                                    sep = "|")
                      )
   
    persist(add_data, "memory")
    
    print(head(add_data)) 
    ### 输出待清洗
    #need_cleaning <- filter(add_data)
}