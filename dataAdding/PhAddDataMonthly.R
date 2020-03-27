

###新的一年无增长率，需要用老增长率代替

c_year <- 2020
c_month <- 1

gr <- read.df(gr_path_online,
              "parquet")
gr$GR1920 = gr$GR1819

printSchema(gr)

#uni_path <- "hdfs://192.168.100.137:8020//common/projects/max/Janssen/universe_sustenna"
universe <- read_universe(uni_path)
id_city <- distinct(universe[, c("PHA", "City", "City_Tier_2010")])

# 1.2 读取CPA与PHA的匹配关系:
if(T){
  cpa_pha_mapping <- map_cpa_pha(
    cpa_pha_mapping_path, universe
  )
  # cpa_pha_mapping <- map_cpa_pha(
  #   "/common/projects/max/Janssen/MappingPha"
  # )
}



# 1.3 读取原始样本数据:
if(T){
  raw_data <- read_raw_data(
    raw_data_path,
    cpa_pha_mapping
  )
  # raw_data <- read_raw_data(
  #   "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Zytiga_Market_201801-201907",
  #   cpa_pha_mapping
  # )
  persist(raw_data, "MEMORY_ONLY")
}

if(F){
  raw_data <- filter(raw_data, raw_data$Year>2016)
}

raw_data <- raw_data %>% filter(raw_data$Month == c_month)


# 1.6 原始数据格式整理:
seed <- trans_raw_data_for_adding(raw_data, id_city, gr)

# 1.7 补充各个医院缺失的月份:
print("start adding data by alfred yang")
persist(seed, "MEMORY_ONLY")
adding_results <- add_data(seed)

adding_data <- adding_results[[1]]


# 1.8 合并补数部分和原始部分:
raw_data_adding <- combind_data(raw_data, adding_data)
raw_data_adding <- repartition(raw_data_adding, 2L)


adding_data_new <- raw_data_adding %>%
    filter(raw_data_adding$Year == c_year)

chk <- agg(groupBy(
    adding_data_new,
    "Year", "add_flag"
),
Sales = "sum"
)
print(head(chk))

# 2. panel


panel <-
  cal_max_data_panel(
    uni_path,
    mkt_path,
    map_path,
    c_month = substr(c_year*100+c_month, 3, 6),
    add_data = adding_data_new,
    min_content,
    need_cleaning_cols
  )
new_hospital <- openxlsx::read.xlsx(new_hospital_path, colNames=F)[[1]]

head(agg(groupBy(
  raw_data, 'Year'
),
Sales = "sum"
))

panel_raw_data <- panel %>% filter(panel$add_flag == 0)
panel_add_data <- panel %>% filter(panel$add_flag == 1)



original_ym_molecule <- distinct(select(panel_raw_data, 
                                        'Date','Molecule'))
original_ym_min2 <- distinct(select(panel_raw_data, 
                                    'Date','Prod_Name'))


panel_add_data <- panel_add_data %>% 
  join(original_ym_molecule, 
       panel_add_data$Date == original_ym_molecule$Date &
         panel_add_data$Molecule == original_ym_molecule$Molecule,
       'inner') %>%
  drop_dup_cols()
panel_add_data <- panel_add_data %>% 
  join(original_ym_min2, 
       panel_add_data$Date == original_ym_min2$Date &
         panel_add_data$Prod_Name == original_ym_min2$Prod_Name,
       'inner') %>%
  drop_dup_cols()

kct <- c('北京市',
         '长春市',
         '长沙市',
         '常州市',
         '成都市',
         '重庆市',
         '大连市',
         '福厦泉市',
         '广州市',
         '贵阳市',
         '杭州市',
         '哈尔滨市',
         '济南市',
         '昆明市',
         '兰州市',
         '南昌市',
         '南京市',
         '南宁市',
         '宁波市',
         '珠三角市',
         '青岛市',
         '上海市',
         '沈阳市',
         '深圳市',
         '石家庄市',
         '苏州市',
         '太原市',
         '天津市',
         '温州市',
         '武汉市',
         '乌鲁木齐市',
         '无锡市',
         '西安市',
         '徐州市',
         '郑州市',
         '合肥市',
         '呼和浩特市',
         '福州市',
         '厦门市',
         '泉州市',
         '珠海市',
         '东莞市',
         '佛山市',
         '中山市')
panel_add_data <- filter(
  panel_add_data,!(
    panel_add_data$City %in% c(
      '北京市',
      '上海市',
      '天津市',
      '重庆市',
      '广州市',
      '深圳市',
      '西安市',
      '大连市',
      '成都市',
      '厦门市',
      '沈阳市'
    )
  ) & !(panel_add_data$Province %in% c('河北省', "福建省")) &
    !(
      !(panel_add_data$City %in% kct) &
        panel_add_data$Molecule %in% c('奥希替尼')
    )
)

not_arrived <- 
  dplyr::bind_rows(openxlsx::read.xlsx("y:/MAX/Sanofi/UPDATE/2001/Not arrived202001.xlsx"),
                   openxlsx::read.xlsx("y:/MAX/Sanofi/UPDATE/1912/Not arrived201912.xlsx"))

unpublished <- openxlsx::read.xlsx("y:/MAX/Sanofi/UPDATE/2001/Unpublished2020.xlsx")
future_range <- unique(rbind(not_arrived, unpublished)) %>% createDataFrame()


panel_add_data_fut <- panel_add_data %>% 
  filter(panel_add_data$Date > 201911)


panel_add_data_fut <- panel_add_data_fut %>% 
  join(future_range, 
       panel_add_data_fut$Date == future_range$Date &
         panel_add_data_fut$ID == future_range$ID,
       'inner') %>%
  drop_dup_cols()


panel_filtered <- rbind(panel_raw_data, panel_add_data_fut)

head(agg(group_by(panel_filtered,'add_flag'), a= count(panel_filtered$add_flag)))
head(agg(groupBy(
  panel_filtered,
  "add_flag"
),
Sales = "sum"
))

if(F){
  kct <- c('北京市',
           '长春市',
           '长沙市',
           '常州市',
           '成都市',
           '重庆市',
           '大连市',
           '福厦泉市',
           '广州市',
           '贵阳市',
           '杭州市',
           '哈尔滨市',
           '济南市',
           '昆明市',
           '兰州市',
           '南昌市',
           '南京市',
           '南宁市',
           '宁波市',
           '珠三角市',
           '青岛市',
           '上海市',
           '沈阳市',
           '深圳市',
           '石家庄市',
           '苏州市',
           '太原市',
           '天津市',
           '温州市',
           '武汉市',
           '乌鲁木齐市',
           '无锡市',
           '西安市',
           '徐州市',
           '郑州市',
           '合肥市',
           '呼和浩特市',
           '福州市',
           '厦门市',
           '泉州市',
           '珠海市',
           '东莞市',
           '佛山市',
           '中山市')
  original_ym_molecule <- distinct(select(panel %>% filter(panel$add_flag == 0), 
                                          'Date','Molecule'))
  original_ym_min2 <- distinct(select(panel %>% filter(panel$add_flag == 0), 
                                      'Date','Prod_Name'))
  panel <- panel %>% join(original_ym_molecule, panel$Date == original_ym_molecule$Date &
                            panel$Molecule == original_ym_molecule$Molecule,
                          'inner') %>%
    drop_dup_cols()
  panel <- panel %>% join(original_ym_min2, panel$Date == original_ym_min2$Date &
                            panel$Prod_Name == original_ym_min2$Prod_Name,
                          'inner') %>%
    drop_dup_cols()
  panel <- filter(panel, !(panel$add_flag == 1 & panel$City %in% c(
    '北京市',
    '上海市',
    '天津市',
    '重庆市',
    '广州市',
    '深圳市',
    '西安市',
    '大连市',
    '成都市',
    '厦门市',
    '沈阳市'
  )) & !(panel$add_flag == 1 & panel$Province %in% c(
    '河北省',"福建省"
  )) & !(panel$add_flag == 1 & panel$Date > 201900 & panel$Date < 202000) &
    !(panel$add_flag == 1 & !(panel$HOSP_ID %in% new_hospital)) &
    !(panel$add_flag == 1 & !(panel$City %in% kct) & 
        panel$Molecule %in% c('奥希替尼')))
  head(agg(group_by(panel,'add_flag'), a= count(panel$add_flag)))
  head(agg(groupBy(
    panel,
    "add_flag"
  ),
  Sales = "sum"
  ))
  
  head(agg(groupBy(
    raw_data
  ),
  Sales = "sum"
  ))
  
}
if(F){
  write.df(panel_filtered, paste0("/common/projects/max/AZ_Sanofi/panel-result_AZ_Sanofi_",
                         c_year*100+c_month), 
           "parquet", "overwrite")
}

write.df(panel_filtered, panel_path, 
         "parquet", "append")
# panel <-
#     cal_max_data_panel(
#         uni_path,
#         mkt_path = "hdfs://192.168.100.137:8020//common/projects/max/Janssen/产品匹配表",
#         map_path = "hdfs://192.168.100.137:8020//common/projects/max/Janssen/产品匹配表",
#         c_month = substr(c_year*100+c_month, 3, 6),
#         add_data = adding_data_new
#     )

