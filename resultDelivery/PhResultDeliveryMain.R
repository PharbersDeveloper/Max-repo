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
    enableHiveSupport = F,
    sparkConfig = list(
        spark.driver.memory = "2g",
        spark.executor.memory = "1g",
        spark.executor.cores = "2",
        spark.executor.instances = "2"
    )
)

source("dataAdding/PhUniverseReading.R", encoding = "UTF-8")
source("dataAdding/PhDropDupCols.R", encoding = "UTF-8")
source('F:/github/Max-repo/resultDelivery/PhCombindMaxData.R')
source('F:/github/Max-repo/resultDelivery/PhMappingProd.R', encoding = 'UTF-8')
source('F:/github/Max-repo/resultDelivery/PhRound.R')


uni_path <- "hdfs://192.168.100.137:8020//common/projects/max/Janssen/universe_sustenna"
universe <- read_universe(uni_path)



c_month <- 9

bed <- universe %>% filter(universe$BEDSIZE >99) %>% 
    select('PHA') %>% distinct()

map_path <- "/common/projects/max/Janssen/产品匹配表"
prod_mapping <- read.df(map_path, 'parquet')


max <- 
    combind_max_data(c("/common/projects/max/Janssen/Zytiga_hosp_MAX_result_1909",
                       "/common/projects/max/Janssen/Sustenna_hosp_MAX_result_1909"),
                     bed, universe, prod_mapping, if_dosage_unit = F)


write.df(max, "/common/projects/max/Janssen/Janssen_MAX_result_1909", 
         "parquet", "overwrite")
    

