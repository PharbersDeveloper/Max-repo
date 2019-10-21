
library("BPRSparkCalCommon")

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
    need_cleaning <- distinct(select(filter(add_data, 
                            !contains(add_data$min1, map$min1)),
                            c("药品名称", "商品名", "剂型", "规格", 
                              "包装数量", "生产企业", "min1")
                           ))

    write.df(need_cleaning, "/Max-repo/need_cleaning", "parquet", "overwrite")
    
    ### Panel
    mp <- select(map, "min1", "min2")
    panel <- join(add_data, mp, add_data$min1 == mp$min1, "left")
    
    mkts <- distinct(select(mkt, "市场", "药品名称"))
    panel <- join(panel, mkts, panel$药品名称 == mkts$药品名称, "left")
    
    panel <- join(panel, uni, panel$新版ID == uni$新版ID)

    panel <- ColRename(
                agg(groupBy(panel, 
                     panel$医院编码,
                     panel$Date,
                     panel$min2,
                     panel$市场,
                     panel$新版名称,
                     panel$新版ID
                    ),
                 Sales = "sum",
                 Units = "sum"),
                c("医院编码", "Date", "min2", "市场", "新版名称", "新版ID"),
                c("ID", "Data", "Prod_Name", "DOI", "Hosp_name", "HOSP_ID"))
    
    panel <- mutate(panel, 
                    Prod_Name = panel$Prod_Name,
                    Strength = panel$Prod_Name,
                    DOIE = panel$DOI)
    
    panel <- partitionBy(panel, panel$Date, panel$DOI)
    
    write.df(panel, "/Map-repo/panel-result", "parquet", "overwrite")
}