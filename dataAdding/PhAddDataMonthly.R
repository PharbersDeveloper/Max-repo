

###新的一年无增长率，需要用老增长率代替。
###更正：无全年增长就用当月同比增长。
###但是要多读取两年的出版名单，在同样医院范围计算。

c_year <- 2020
first_adding_month <- 1
c_month <- 4

others <- F


not_arrived_path <- paste0("y:/MAX/Sanofi/UPDATE/",
                          substr(c_year*100+c_month,3,6),
                          "/Not arrived", c_year*100+c_month,".xlsx")


published_l <- openxlsx::read.xlsx(published_l_path)
published_r <- openxlsx::read.xlsx(published_r_path)
if(F){
  not_arrived <- 
    dplyr::bind_rows(openxlsx::read.xlsx("y:/MAX/Sanofi/UPDATE/2001/Not arrived202001.xlsx"),
                     openxlsx::read.xlsx("y:/MAX/Sanofi/UPDATE/1912/Not arrived201912.xlsx"))
  
}

not_arrived <- openxlsx::read.xlsx(not_arrived_path)


universe <- read_universe(uni_base_path)
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
if(!others){
  raw_data <- read_raw_data(
    raw_data_path,
    cpa_pha_mapping
  )
  # raw_data <- read_raw_data(
  #   "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Zytiga_Market_201801-201907",
  #   cpa_pha_mapping
  # )
  persist(raw_data, "MEMORY_ONLY")
}else{
  raw_data <- read_raw_data(
    others_box_path,
    cpa_pha_mapping
  )
}

raw_data <- join(raw_data, id_city,
                 raw_data$PHA == id_city$PHA, 'left') %>%
  drop_dup_cols()

full_product <- ifelse(!isNull(raw_data$Brand),
                       raw_data$Brand, raw_data$Molecule)


raw_data <- mutate(
  raw_data,
  Brand = full_product
)
raw_data <- concat_multi_cols(raw_data, min_content,
                              'min1',
                              sep = min1_sep)
map <- read.df(map_path, "parquet")
names(map)[names(map) %in% '标准通用名'] <- '通用名'
names(map)[names(map) %in% '标准途径'] <- 'std_route'
if(!('std_route' %in% names(map))){
  map$std_route <- lit('')
}
map1 <- distinct(select(map, 'min1'))
mp <- distinct(select(map, "min1", "min2", "通用名", 'std_route','标准商品名'))
need_cleaning <- distinct(select(raw_data %>%
                                   join(map1, raw_data$min1== map1$min1,'left_anti') %>%
                                   drop_dup_cols(),
                                 need_cleaning_cols
))

print(nrow(need_cleaning))
if(nrow(need_cleaning)>0){
  # need_cleaning_path = 
  #     paste0('Y:/MAX/Janssen/UPDATE/',c_month,'/待清洗',Sys.Date(),'.xlsx')
  print(paste("已输出待清洗文件至", need_cleaning_path))
  
  openxlsx::write.xlsx(collect(need_cleaning), 
                       need_cleaning_path)
}


raw_data <- join(raw_data, mp, raw_data$min1 == mp$min1, "left") %>%
  drop_dup_cols()
raw_data <- drop(raw_data, 'S_Molecule')
names(raw_data)[names(raw_data) == c('通用名')] <- 'S_Molecule'


poi = openxlsx::read.xlsx(poi_path)[[1]]

raw_data$S_Molecule_for_gr <- ifelse(raw_data$标准商品名 %in% poi,
                                     raw_data$标准商品名,
                                     raw_data$S_Molecule)


##补数部分的数量需要用价格得出
price <- cal_price(raw_data)

price <- repartition(price, 2L)


if(!others){
  write.df(price, price_path, 'parquet', 'overwrite')
}else{
  write.df(price, price_box_path, 'parquet', 'overwrite')
}


if(F){
  raw_data <- filter(raw_data, raw_data$Year>2016)
}


index <- 1
for(m in first_adding_month:c_month){
  print(m)
  same_hosp <- intersect(published_l[[1]], published_r[[1]]) %>%
    setdiff(not_arrived$ID[not_arrived$Date == c_year*100+m])
  
  raw_data_m <- raw_data %>% filter(raw_data$Month == m)
  
  
  
  gr_all <- cal_growth(raw_data_m %>% 
                         filter((raw_data_m$ID %in% same_hosp)))
  gr <- gr_all[[1]]
  # 1.6 原始数据格式整理:
  seed <- trans_raw_data_for_adding(raw_data_m, gr)
  
  # 1.7 补充各个医院缺失的月份:
  print("start adding data by alfred yang")
  #persist(seed, "MEMORY_ONLY")
  
  
  if(!others){
    adding_results <- add_data(seed, price_path)
  }else{
    adding_results <- add_data(seed, price_box_path)
  }
  
  if(index == 1){
    adding_data <- adding_results[[1]]
    index <- index + 1
  }else{
    adding_data <- rbind(adding_data, adding_results[[1]])
  }
  
  
  
}




# gr <- read.df(gr_path_online,
#               "parquet")
# 
# ##
# gr$GR1920 = gr$GR1819

# printSchema(gr)

#uni_path <- "hdfs://192.168.100.137:8020//common/projects/max/Janssen/universe_sustenna"






# 1.8 合并补数部分和原始部分:
raw_data_adding <- combind_data(raw_data, adding_data)
#raw_data_adding <- repartition(raw_data_adding, 2L)




adding_data_new <- raw_data_adding %>%
    filter(raw_data_adding$Year == c_year)
if(F){
  adding_data_new <- raw_data_adding %>%
    filter(raw_data_adding$Year == c_year &
             raw_data_adding$Month == c_month)
}

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
    uni_base_path,
    mkt_path,
    add_data = adding_data_new
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
    ) & !(panel_add_data$Province %in% c('河北省', "福建省",'河北', "福建")) &
      !(
        !(panel_add_data$City %in% kct) &
          panel_add_data$Molecule %in% c('奥希替尼')
      )
  )
}

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
  ) & !(panel_add_data$Province %in% c('河北省', "福建省",'河北', "福建"))
)

unpublished <- openxlsx::read.xlsx(unpublished_path)
future_range <- unique(rbind(not_arrived, unpublished)) %>% createDataFrame()


panel_add_data_fut <- panel_add_data %>% 
  filter(panel_add_data$Date > model_month_r)


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
  write.df(panel_filtered, paste0("/common/projects/max/AZ_Sanofi/panel-result_AZ_Sanofi_",
                         c_year*100+c_month), 
           "parquet", "overwrite")
  panel_filtered <- repartition(panel_filtered, 2L)
  write.df(panel_filtered, panel_path, 
           "parquet", "append")
}

# panel <-
#     cal_max_data_panel(
#         uni_path,
#         mkt_path = "hdfs://192.168.100.137:8020//common/projects/max/Janssen/产品匹配表",
#         map_path = "hdfs://192.168.100.137:8020//common/projects/max/Janssen/产品匹配表",
#         c_month = substr(c_year*100+c_month, 3, 6),
#         add_data = adding_data_new
#     )

