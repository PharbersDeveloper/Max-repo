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



# 1.3 读取所有原始样本数据:
if(F){
  raw_data <- combind_raw_data(
    c("hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Zytiga_Market_201801-201910",
      "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Sustenna_Market_201801-201910"),
    cpa_pha_mapping
  )
  persist(raw_data, "MEMORY_ONLY")
}


raw_data <- sql("select * from CPA_Janssen where company = 'Janssen'")
print(head(raw_data))

raw_data <- format_raw_data(raw_data)

# 1.4 计算样本医院连


# 1.5 计算样本分子增长率:

#gr_with_id <- gr_all[[2]]
gr_with_id <- read.df("hdfs://192.168.100.137:8020//common/projects/max/Janssen/gr_with_id",
                      "parquet")

names(gr_with_id) <- c("PHA", "ID", "City", "CITYGROUP", "Molecule",
                       "Year_2018", "Year_2019", "GR1819")

printSchema(gr_with_id)

c_year <- 2019
c_month <- 10

raw_data <- raw_data %>% filter(raw_data$Month == c_month)


# 1.6 原始数据格式整理:
seed <- trans_raw_data_for_adding(raw_data, id_city, gr_with_id)

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
        mkt_path = "hdfs://192.168.100.137:8020//common/projects/max/Janssen/产品匹配表",
        map_path = "hdfs://192.168.100.137:8020//common/projects/max/Janssen/产品匹配表",
        c_month = substr(c_year*100+c_month, 3, 6),
        add_data = adding_data_new
    )

write.df(panel %>% filter(panel$DOI == "Zytiga"), 
         paste0("/common/projects/max/Janssen/panel-result_Zytiga_20",
                substr(c_year*100+c_month, 3, 6)), 
         "parquet", "overwrite")

write.df(panel %>% filter(panel$DOI == "Sustenna"), 
         paste0("/common/projects/max/Janssen/panel-result_Sustenna_20",
                substr(c_year*100+c_month, 3, 6)), 
         "parquet", "overwrite")
