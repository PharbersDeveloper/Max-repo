# Max-repo
print("start max job")
library(SparkR)
library(magrittr)
#library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#library(uuid)
library(BPRSparkCalCommon)
#library(RKafkaProxy)

cmd_args = commandArgs(T)

Sys.setenv(SPARK_HOME="/Users/alfredyang/Desktop/spark/spark-2.3.0-bin-hadoop2.7")
Sys.setenv(YARN_CONF_DIR="/Users/alfredyang/Desktop/hadoop-3.0.3/etc/hadoop")

ss <- sparkR.session(
    appName = "Max Cal",
    enableHiveSupport = F,
    sparkConfig = list(
        spark.driver.memory = "2g",
        spark.executor.memory = "2g",
        spark.executor.cores = "2",
        spark.executor.instances = "3")
)

#source("dataPre/PhDataPre.R")
source("dataAdding/PhDataAddingJ.R", encoding = "UTF-8")
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

source("panel/PhPanelGen.R", encoding = "UTF-8")

# cal_J_data_pre()

# 1. 首次补数
# 1.1 读取新版PHA与城市、城市等级、老版PHA的匹配表:
map_city_id <- cal_data_adding_for_J(
    "\\Map-repo\\2019年Universe更新维护1.0_190403\\Universe2019"
    )

id_city <- map_city_id[[1]]
pha_id_transfer <- map_city_id[[2]]

# 1.2 读取CPA与PHA的匹配关系:
map_cpa_pha <- map_cpa_pha(
    "\\Map-repo\\2019年Universe更新维护1.0_190403\\Mapping",
    "\\Map-repo\\CPA_VS_GYC_VS_PHA_VS_HH_0418",
    pha_id_transfer
)



# 1.3 读取原始样本数据:
raw_data <- read_raw_data("\\Map-repo\\190814泰德-1701-1906检索2\\1701-1906",
                          map_cpa_pha)
persist(raw_data, "MEMORY_ONLY")

# 1.4 计算样本医院连续性:
con_all <- cal_continuity(raw_data)

con <- con_all[[2]]


# 1.5 计算样本分子增长率:
gr_all <- cal_growth(raw_data, id_city)
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
write.parquet(raw_data_adding, "\\Map-repo\\raw_data_adding", mode = "overwrite")
unpersist(seed, blocking = FALSE)
unpersist(raw_data, blocking = FALSE)

# 1.9 进一步为最后一年独有的医院补最后一年的缺失月份
#      （可能也要考虑第一年）:

adding_data_new <- add_data_new_hosp(raw_data_adding_path, original_range)
# persist(adding_data_new, "MEMORY_AND_DISK")
# unpersist(original_range, blocking = FALSE)

# 1.10 检查补数占比:


# 1.11 输出补数结果:
# print(head(adding_data_new))
# print(count(adding_data_new))
write.parquet(adding_data_new, "\\Map-repo\\adding_data_result", mode = "overwrite")
add_res <- read.df("\\Map-repo\\adding_data_result", "parquet")

chk = agg(groupBy(
                    add_res, 
                    "Year", "add_flag"
                 ),
          Sales = "sum"
         )
print(head(chk))

# 2. panel

panel <-
    cal_max_data_panel(
        uni_2019_path = "\\Map-repo\\2019年Universe更新维护1.0_190403\\Universe2019",
        mkt_path = "/Map-repo/通用名企业层面集中度_pb",
        map_path = "/Map-repo/泰德产品匹配表",
        c_month = "1906",
        add_data = add_res
    )
