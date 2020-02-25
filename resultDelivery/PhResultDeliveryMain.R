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

source("dataAdding/PhUniverseReading.R", encoding = "UTF-8")
source("dataAdding/PhDropDupCols.R", encoding = "UTF-8")
source('F:/github/Max-repo/resultDelivery/PhCombindMaxData.R', encoding = "UTF-8")
source('F:/github/Max-repo/resultDelivery/PhMappingProd.R', encoding = 'UTF-8')
source('F:/github/Max-repo/resultDelivery/PhRound.R', encoding = "UTF-8")
source('maxCal/PhRemoveNegativeValue.R', encoding = "UTF-8")




time <- 201910
all_mkt <- c('Zytiga','Sustenna')


##这个地方的universe用来提供100床位，一般可以不改市场，
##但是有些项目不同市场的样本不同，这里以后还是要改成不固定
uni_path <- "hdfs://192.168.100.137:8020//common/projects/max/Janssen/universe_Sustenna"
universe <- read_universe(uni_path)



#线下的空床位会让结果出错，线上不会
bed <- universe %>% filter(universe$BEDSIZE >99) %>% 
    select('PHA') %>% distinct()

map_path <- "/common/projects/max/Janssen/产品匹配表"
prod_mapping <- read.df(map_path, 'parquet')





max <- 
    combind_max_data(paste0("/common/projects/max/Janssen/Janssen_MAX_result_",
                            time,all_mkt, "_hosp_level"),
                     bed, universe, prod_mapping, if_dosage_unit = F)


agg(max,a=sum(ifelse(isNull(max$Date),1,0))) %>% head()
agg(max,金额='sum') %>% head()

write.df(max, paste0("/common/projects/max/Janssen/Janssen_MAX_result_",
                     time), 
         "parquet", "overwrite")
if(F){
    max_co <- collect(max)
    
    colSums(is.na(max_co))
    
    openxlsx::write.xlsx(max_co[max_co$ATC3 %in% 'N05A',],
                         paste0('Y:/MAX/Janssen/UPDATE/',
                                substr(time, 3,6),'/MAX_', 'Sustenna',
                                substr(time, 3,6),'.xlsx'))
    openxlsx::write.xlsx(max_co[!max_co$ATC3 %in% 'N05A',],
                         paste0('Y:/MAX/Janssen/UPDATE/',
                                substr(time, 3,6),'/MAX_', 'Zytiga',
                                substr(time, 3,6),'.xlsx'))
}
    

