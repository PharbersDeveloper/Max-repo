# Max-repo
print("start max job")
#library(SparkR)
library(magrittr)
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#library(uuid)
#library(BPRSparkCalCommon)
#library(RKafkaProxy)

cmd_args = commandArgs(T)

Sys.setenv(SPARK_HOME="D:/tools/spark-2.3.0-bin-hadoop2.7")
Sys.setenv(YARN_CONF_DIR="D:/tools/hadoop-3.0.3/etc/hadoop/")

ss <- sparkR.session(
    appName = "Max Cal",
    sparkConfig = list(
        spark.driver.memory = "1g",
        spark.executor.memory = "2g",
        spark.executor.cores = "2",
        spark.executor.instances = "2")
)

#source("dataPre/PhDataPre.R")
source("dataAdding/PhDataAddingJ.R",encoding = 'UTF-8')
source("dataAdding/PhCpaPhaMapping.R",encoding = 'UTF-8')
source("dataAdding/PhReadRawData.R",encoding = 'UTF-8')

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

# 1.4 计算样本医院连续性:


# 1.5 计算样本分子增长率:


# 1.6 原始数据格式整理:


# 1.7 补充各个医院缺失的月份:


# 1.8 检查补数部分的时间范围:


# 1.9 合并补数部分和原始部分:


# 1.10 进一步为最后一年独有的医院补最后一年的缺失月份
#      （可能也要考虑第一年）:


# 1.11 检查补数占比:


# 1.12 输出补数结果:




# 2. 


