# Max-repo
print("start max job")
library(SparkR)
library(magrittr)
# library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
# library(uuid)
library(BPRSparkCalCommon)
library(styler)
# library(RKafkaProxy)

cmd_args <- commandArgs(T)

Sys.setenv(SPARK_HOME = "D:/tools/spark-2.3.0-bin-hadoop2.7")
Sys.setenv(YARN_CONF_DIR = "D:/tools/hadoop-2.7.3/etc/hadoop")

ss <- sparkR.session(
  appName = "Max Cal",
  enableHiveSupport = T,
  sparkConfig = list(
    spark.driver.memory = "2g",
    spark.executor.memory = "1g",
    spark.executor.cores = "2",
    spark.executor.instances = "2"
  )
)

# source("dataPre/PhDataPre.R")
source("dataAdding/PhUniverseReading.R", encoding = "UTF-8")
source("dataAdding/PhCpaPhaMapping.R", encoding = "UTF-8")
source("dataAdding/PhReadRawData.R", encoding = "UTF-8")
source("dataAdding/PhContinuity.R", encoding = "UTF-8")
source("dataAdding/PhGrowth.R", encoding = "UTF-8")
source("dataAdding/PhCompareColumnsByRow.R", encoding = "UTF-8")
source("dataAdding/PhDropDupCols.R", encoding = "UTF-8")
source("dataAdding/PhRawDataTransformation.R", encoding = "UTF-8")
source("dataAdding/PhAddData.R", encoding = "UTF-8")
source("dataAdding/PhAddDataMultiplyGrowth.R", encoding = "UTF-8")
source("dataAdding/PhCombindData.R", encoding = "UTF-8")
source("dataAdding/PhAddDataNewHosp.R", encoding = "UTF-8")
source('dataAdding/PhFormatRawData.R', encoding = 'UTF-8')
source("panel/PhPanelGen.R", encoding = "UTF-8")
source('dataAdding/PhCombindRawData.R', encoding = 'UTF-8')
source('dataAdding/PhParameters.R', encoding = 'UTF-8')
source('dataAdding/PhAddGrCols.R', encoding = 'UTF-8')
source('dataAdding/PhSetSchema.R', encoding = 'UTF-8')
source('dataAdding/PhSumMultiCols.R', encoding = 'UTF-8')


# 1. 首次补数

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
  raw_data <- filter(raw_data, raw_data$Year>2016 & raw_data$Year<2020)
}

if(F){
  raw_data <- sql("select * from CPA_Janssen where company = 'Janssen'")
  print(head(raw_data))
  raw_data <- format_raw_data(raw_data)
}

# 1.4 计算样本医院连续性:
if(T){
  con_all <- cal_continuity(raw_data)
  con_dis <- con_all[[1]]
  con <- con_all[[2]]
}

if(F){
  openxlsx::write.xlsx(collect(con), 
                       continuity_path)
}


# 1.5 计算样本分子增长率:
if(T){
  #完整年
  gr_all_p1 <- cal_growth(raw_data %>% 
                         filter(!(raw_data$Year %in% year_missing)), 
                       id_city)
  gr_p1 <- gr_all_p1[[1]]
  gr_with_id_p1 <- gr_all_p1[[2]]
}

if(T){
  #不完整年
  gr_all_p2 <- cal_growth(raw_data %>% 
                         filter((raw_data$Year %in% 
                                   c(year_missing, year_missing-1,
                                     year_missing+1))), 
                       id_city, max_month)
  gr_p2 <- gr_all_p2[[1]]
  gr_with_id_p2 <- gr_all_p2[[2]]
}


if(F){
  
  gr=rbind(gr_p1[,c('S_Molecule','CITYGROUP')], 
           gr_p2[,c('S_Molecule','CITYGROUP')]) %>%
    distinct()
  print(nrow(gr))
  gr <- join(gr,
             gr_p1[, c('S_Molecule', 'CITYGROUP',
                       names(gr_p1)[startsWith(names(gr_p1), 'GR')])],
             gr$S_Molecule == gr_p1$S_Molecule &
               gr$CITYGROUP == gr_p1$CITYGROUP,
             'left') %>% drop_dup_cols()
  gr <- join(gr,
             gr_p2[, c('S_Molecule', 'CITYGROUP',
                       names(gr_p2)[startsWith(names(gr_p2), 'GR')])],
             gr$S_Molecule == gr_p2$S_Molecule &
               gr$CITYGROUP == gr_p2$CITYGROUP,
             'left') %>% drop_dup_cols()
  print(nrow(gr))
  
  write.parquet(gr, gr_path_online, mode = "overwrite")
  # gr_with_id <- distinct(rbind(gr_with_id_p1[,
  #                                            c("PHA",'ID',
  #                                              'City','CITYGROUP',
  #                                              'Molecule',
  #                                              'S_Molecule')],
  #                              gr_with_id_p2[,
  #                                            c("PHA",'ID',
  #                                              'City','CITYGROUP',
  #                                              'Molecule',
  #                                              'S_Molecule')]))
  # print(nrow(gr_with_id))
  # 
  # gr_with_id <- 
  #   join(gr_with_id, 
  #        gr_with_id_p1[,
  #                      names(gr_with_id_p1)[!startsWith(names(gr_with_id_p1), 'GR')]],
  #        gr_with_id$PHA == gr_with_id_p1$PHA &
  #          gr_with_id$ID == gr_with_id_p1$ID &
  #        gr_with_id$Molecule == gr_with_id_p1$Molecule,
  #        'left') %>%
  #   drop_dup_cols()
  # print(nrow(gr_with_id))
  # gr_with_id <- 
  #   join(gr_with_id, 
  #        gr_with_id_p2[,
  #                      names(gr_with_id_p2)[!startsWith(names(gr_with_id_p2), 'GR')]],
  #        gr_with_id$PHA == gr_with_id_p2$PHA &
  #          gr_with_id$ID == gr_with_id_p2$ID &
  #        gr_with_id$Molecule == gr_with_id_p2$Molecule,
  #        'left') %>%
  #   drop_dup_cols()
  # gr_with_id <- 
  #   join(gr_with_id, 
  #        gr,
  #        gr_with_id$CITYGROUP == gr$CITYGROUP &
  #          gr_with_id$S_Molecule == gr$S_Molecule,
  #        'left') %>%
  #   drop_dup_cols()
  # print(nrow(gr_with_id))
  # openxlsx::write.xlsx(collect(gr_with_id), 
  #                      gr_path)
}

# 1.6 原始数据格式整理:
seed <- trans_raw_data_for_adding(raw_data, id_city, gr)

# 1.7 补充各个医院缺失的月份:
print("start adding data by alfred yang")
persist(seed, "MEMORY_ONLY")
adding_results <- add_data(seed)

adding_data <- adding_results[[1]]
original_range <- adding_results[[2]]
persist(original_range, "MEMORY_ONLY")

# 1.8 合并补数部分和原始部分:
raw_data_adding <- combind_data(raw_data, adding_data)
raw_data_adding <- repartition(raw_data_adding, 2L)


##好神奇，写出去再读进来，driver机就不会内存溢出了
if(T){
  
  write.parquet(raw_data_adding, raw_data_adding_path, mode = "overwrite")
  raw_data_adding <- read.df(raw_data_adding_path, "parquet")
  
  write.parquet(original_range, original_range_path, mode = "overwrite")
  original_range <- read.df(original_range_path, "parquet")
  
}


unpersist(seed, blocking = FALSE)
unpersist(raw_data, blocking = FALSE)

# 1.9 进一步为最后一年独有的医院补最后一年的缺失月份
#      （可能也要考虑第一年）:

adding_data_new_result <- add_data_new_hosp(raw_data_adding, original_range)
adding_data_new <- adding_data_new_result[[1]]
new_hospital <- adding_data_new_result[[2]]

if(F){
  openxlsx::write.xlsx(new_hospital, new_hospital_path)
  new_hospital <- openxlsx::read.xlsx(new_hospital_path, colNames=F)[[1]]
}
# persist(adding_data_new, "MEMORY_AND_DISK")
# unpersist(original_range, blocking = FALSE)

# 1.10 检查补数占比:


# 1.11 输出补数结果:
if(T){
  
  write.parquet(adding_data_new, adding_data_new_path, mode = "overwrite")
  adding_data_new <- read.df(adding_data_new_path, "parquet")
  
}
# print(head(adding_data_new))
# print(count(adding_data_new))
#write.parquet(adding_data_new, "\\Map-repo\\adding_data_result", mode = "overwrite")
#add_res <- read.df("\\Map-repo\\adding_data_result", "parquet")

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
    c_month,
    add_data = adding_data_new,
    min_content,
    need_cleaning_cols
  )

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

#早于model所用时间（历史数据），用new_hospital补数;
#处于model所用时间（模型数据），不补数；
#晚于model所用时间（月更新数据），用unpublished和not arrived补数

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

panel_add_data_his <- panel_add_data %>%
  filter(panel_add_data$HOSP_ID %in% new_hospital &
      panel_add_data$Date < 201901
  )
panel_add_data_fut <- panel_add_data %>% 
  filter(panel_add_data$Date > 201911)

panel_add_data_fut <- panel_add_data_fut %>% 
  join(future_range, 
       panel_add_data_fut$Date == future_range$Date &
         panel_add_data_fut$ID == future_range$ID,
       'inner') %>%
  drop_dup_cols()




panel_filtered <- rbind(panel_raw_data, panel_add_data_his) %>%
  rbind(panel_add_data_fut)

head(agg(group_by(panel_filtered,'add_flag'), a= count(panel_filtered$add_flag)))
head(agg(groupBy(
  panel_filtered,
  "add_flag"
),
Sales = "sum"
))

head(agg(groupBy(
  raw_data
),
Sales = "sum"
))


if(F){
  
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
  )) & !(panel$add_flag == 1 & panel$Date > 201900 & panel$Date < 201912) &
    !(panel$add_flag == 1 & !(panel$HOSP_ID %in% new_hospital) & 
        panel$Date < 201912) &
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
write.df(panel_filtered, panel_path, 
         "parquet", "overwrite")


if(F){
  panel_filtered = read.df(panel_path, "parquet")
  # panel = panel %>% filter(panel$Date>201900)
  #panel_c <- filter(panel, panel$Date>201900)
  
  
  panel_c <- panel_filtered %>%
    group_by('ID', 'Date', 'DOI', 'Hosp_name', 'HOSP_ID', 'Province', 'City', 
             'add_flag') %>%
    summarize(Sales = sum(panel_filtered$Sales),
              Units = sum(panel_filtered$Units))
  panel_c <- collect(panel_c)
  
  panel_path_local <- paste0('Y:/MAX/AZ/UPDATE/','2001',
                             '/panel_hosp+ym_level_201701-202001_',Sys.Date(),'.csv')
  write.csv(panel_c, panel_path_local, row.names = F)
  
  
  
  panel_c <- filter(panel_filtered, panel_filtered$Date>201900 &
                      panel_filtered$Date<201912)
  panel_c <- panel_c %>%
    group_by('Prod_Name', 'DOI', 'Molecule', 'add_flag') %>%
    summarize(sales201911YTD = sum(panel_c$Sales))
  panel_c <- collect(panel_c)
  openxlsx::write.xlsx(panel_c, 
                       paste0("Y:/MAX/AZ/UPDATE/1912/panel_without_hosp_201911YTD_",
                              Sys.Date(),".xlsx"))
  
  panel_c <- panel_c %>%
    group_by('Prod_Name', 'DOI', 'Molecule', 'add_flag','Province','City') %>%
    summarize(sales201911YTD = sum(panel_c$Sales),
              units201911YTD = sum(panel_c$Units))
  panel_c <- collect(panel_c)
  openxlsx::write.xlsx(panel_c, 
                       paste0("w:/LJX_Backup/提数/202003/tmp.xlsx"))
  
}
