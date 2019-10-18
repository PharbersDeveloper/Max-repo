# Max-repo

print("start max job")
library(SparkR)
library(uuid)
library(BPRSparkCalCommon)
library(RKafkaProxy)

cmd_args = commandArgs(T)

Sys.setenv(SPARK_HOME="/Users/alfredyang/Desktop/spark/spark-2.3.0-bin-hadoop2.7")
Sys.setenv(YARN_CONF_DIR="/Users/alfredyang/Desktop/hadoop-3.0.3/etc/hadoop/")

ss <- sparkR.session(
    appName = "Max Cal",
    sparkConfig = list(
        spark.driver.memory = "1g",
        spark.executor.memory = "2g",
        spark.executor.cores = "2",
        spark.executor.instances = "2")
)

source("dataPre/dataPre.R")
source("dataAdding/PhDataAddingJ.R")

# 0. 数据准备
# cal_excel_data_to_parquet("/Users/alfredyang/Desktop/code/pharbers/Max-repo/tmp/2019年Universe更新维护1.0_190403.xlsx", "Universe2019")

# 1. 首次补数
# 1.1 TODO:
cal_data_adding_for_J("hdfs://192.168.100.137:9000//Map-repo/2019年Universe更新维护1.0_190403/Universe2019")

# 2. 