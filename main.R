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
source('dataAdding/PhCombindRawData.R')



# 1. 首次补数

uni_path <- "hdfs://192.168.100.137:8020//common/projects/max/Janssen/universe_sustenna"
universe <- read_universe(uni_path)
id_city <- distinct(universe[, c("PHA", "City", "City_Tier_2010")])

# 1.2 读取CPA与PHA的匹配关系:
if(F){
  cpa_pha_mapping <- map_cpa_pha(
    "/common/projects/max/Janssen/MappingPha"
  )
}



# 1.3 读取原始样本数据:
if(F){
  raw_data <- read_raw_data(
    "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Zytiga_Market_201801-201907",
    cpa_pha_mapping
  )
  persist(raw_data, "MEMORY_ONLY")
}



raw_data <- sql("select * from CPA_Janssen where company = 'Janssen'")
print(head(raw_data))
raw_data <- format_raw_data(raw_data)

# 1.4 计算样本医院连续性:
con_all <- cal_continuity(raw_data)

con <- con_all[[2]]


# 1.5 计算样本分子增长率:
gr_all <- cal_growth(raw_data, id_city, max_month = 7)
gr <- gr_all[[1]]
gr_with_id <- gr_all[[2]]


# 1.6 原始数据格式整理:
seed <- trans_raw_data_for_adding(raw_data, id_city, gr_with_id)

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
if(F){
  raw_data_adding_path <- "/common/projects/max/Janssen/raw_data_adding"
  write.parquet(raw_data_adding, raw_data_adding_path, mode = "overwrite")
  raw_data_adding <- read.df(raw_data_adding_path, "parquet")
  
}


unpersist(seed, blocking = FALSE)
unpersist(raw_data, blocking = FALSE)

# 1.9 进一步为最后一年独有的医院补最后一年的缺失月份
#      （可能也要考虑第一年）:

adding_data_new <- add_data_new_hosp(raw_data_adding, original_range)
# persist(adding_data_new, "MEMORY_AND_DISK")
# unpersist(original_range, blocking = FALSE)

# 1.10 检查补数占比:


# 1.11 输出补数结果:
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
    mkt_path = "hdfs://192.168.100.137:8020//common/projects/max/Janssen/产品匹配表",
    map_path = "hdfs://192.168.100.137:8020//common/projects/max/Janssen/产品匹配表",
    c_month = "1908",
    add_data = adding_data_new
  )

write.df(panel, "/common/projects/max/Janssen/panel-result_Zytiga_201801-201908", 
         "parquet", "overwrite")
