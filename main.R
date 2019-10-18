# Max-repo

print("start max job")
library(SparkR)
library(uuid)
library(BPRSparkCalCommon)
library(RKafkaProxy)

cmd_args = commandArgs(T)

ss <- sparkR.session(
    appName = "Max Cal",
    sparkConfig = list(
        spark.driver.memory = "1g",
        spark.executor.memory = "2g",
        spark.executor.cores = "2",
        spark.executor.instances = "2")
)

# 1. 首次补数


# 2. 