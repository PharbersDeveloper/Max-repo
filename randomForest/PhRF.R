
library(BPRSparkCalCommon)

cal_ph_random_forest <- function(hosp_range_path, raw_data_path, mot_bt_path, doctor_path, ind_path, mkt = "凯纷") {
    hosp_range <- read.df(hosp_range_path, "parquet")
    persist(hosp_range, "MEMORY_ONLY")
    hosp_range_sample <- distinct(filter(hosp_range, hosp_range$PANEL == 1 & hosp_range$BEDSIZE > 99))
    
    raw_data <- read.df(raw_data_path, "parquet")
    persist(raw_data, "MEMORY_ONLY")
    
    #### 补全PHA.ID
    raw_data_m <- raw_data
    raw_data_m <- ColRename(raw_data_m, c("HOSP_ID"), c("PHA_ID"))
    
    #### 得到因变量
    hosp_range_join <- distinct(filter(hosp_range, hosp_range$PANEL == 1))
    raw_data_i = join(hosp_range_join, raw_data_m, hosp_range_join$Panel_ID == raw_data_m$PHA_ID, "left")
    # TODO: 可能缺列
    raw_data_i = agg(groupBy(raw_data_i, "PHA_ID", "DOI"),
                        Sales = "sum" 
                    )
    
    moh_bt <- read.df(mot_bt_path, "parquet")
    # TODO: groupBy 的列不对
    moh_bt_m <- ColRename(agg(groupBy(moh_bt, "PHA"), BT = "first"), c("first(BT)"), c("BT"))
    
    #### 读取课室信息
    doctor <- read.df(doctor_path, "parquet")
    print(head(doctor))
    # TODO: select c[4:10] 魔法数字不可以存在，写出列名确保通用性
    doctor_m <- select(filter(doctor, !isNull(doctor$BT_Code)), 
                       # "Province", "City", "Hospital_Name", 
                       "Department", "Dr_N", "Dr_N_主任", "Dr_N_副主任", 
                       "Dr_N_主治", "Dr_N_住院医", "BT_Code")
    
    doctor_g <- cal_gather_col(doctor_m)
    print(head(doctor_g))

    doctor_g <- mutate(doctor_g, 
                       dr1 = concat(doctor_g$Department, doctor_g$dr)
                      )
   
    # doctor_g <- filter(doctor_g, !isNull(doctor_g$no))
    doctor_g <- ColRename(agg(groupBy(doctor_g, "BT_Code", "dr1"),
                    no = "sum"
                   ), c("sum(no)"), c("no"))
    
    # doctor_g <- filter(doctor_g, !isNull(doctor_g$no))
    print(count(doctor_g))
    
    doctor_g <- cal_spread_row(doctor_g)
    
    print(head(doctor_g))
    print(count(doctor_g))
    
    #### 读取ind1表
    ind <- read.df(ind_path, "parquet")
}

# 无参数化，未来扩展, 实现行转列
cal_gather_col <- function(df) {
    # df <- repartition(df, 3L, df$dr)
    schema <- structType(
                structField("Department", "string"),
                structField("BT_Code", "string"), 
                structField("dr", "string"),
                structField("no", "double")
              )
    df <- dapply(df, function(x) {
                library("tidyverse")
                x <- gather(x, dr, no, -c(BT_Code, Department))
                return(x)
          }, schema)
    return(df)
}

# 无参数化，未来扩展，实现列转行
cal_spread_row <- function(df) {
    print(head(df))

    ldf <- collect(df)
    library("tidyverse")

    # TODO: 零时解决方案，很可能超过单机处理能力，不过MAX应该够用
    ldf <- spread(ldf, dr1, no, fill = 0)
    df <- createDataFrame(ldf)

    detach("package:tidyverse")
    detach("package:dplyr")

    # persist(df, "MEMORY_ONLY")

    # 行转列需要优先数据分区，不然会出错
    # tmpdf <- arrange(df, df$dr1)
    # tmpdf <- collect(distinct(select(df, "dr1")))
    
    # TODO: schema 需要添加原有schema 不然会出错
    # function_params <- list(structField("BT_Code", "string"))
    # for (i in 1:length(tmpdf[,1])) {
    #     function_params[[i+1]] <- structField(tmpdf[i,1], "double")
    # }
    # schema <-  do.call(SparkR::structType, function_params)
    # print(schema)
    # df <- arrange(repartition(df, 3L, df$BT_Code), df$BT_Code)
    # df <- dapply(df, function(x) {
    #             library("tidyverse")
    #             x <- spread(x, dr1, no, fill = 0)
    #             return(x)
    #       }, schema)
    
    return(df)
}

cal_ph_random_forest("/Map-repo/universe_kaifeng",
                     "/Map-repo/凯纷_Panel_2018",
                     "/Map-repo/去重3",
                     "/Map-repo/副本医院潜力+-+医院范围，重点科室+医生数",
                     "/Map-repo/ind1")