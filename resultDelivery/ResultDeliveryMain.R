print("start max job")
library(SparkR)
library(magrittr)
#library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
#library(uuid)
library(BPRSparkCalCommon)
#library(RKafkaProxy)

cmd_args = commandArgs(T)

Sys.setenv(SPARK_HOME="D:\\tools\\spark-2.3.0-bin-hadoop2.7")
Sys.setenv(YARN_CONF_DIR="D:\\tools\\hadoop-3.0.3\\etc\\hadoop")

ss <- sparkR.session(
    appName = "Max Cal",
    enableHiveSupport = F,
    sparkConfig = list(
        spark.driver.memory = "4g",
        spark.executor.memory = "2g",
        spark.executor.cores = "2",
        spark.executor.instances = "3")
)

#source("dataPre/PhDataPre.R")


# 读取匹配表
universe <- read.df()

map <- read.df(map_path, "parquet")

bed <- select(universe %>% filter(universe$BEDSIZE > 99), 'Panel_ID')

map$Pack_ID <- stri_pad_left(map$Pack_ID,7,'0')


for(mkt in c('凯纷','凯那','凯时','维格列汀片')){
    print(mkt)
    data_all <- NULL
    for(i in c_month){
        print(i)
        tmp <- read.df()
        data_all <- rbind(data_all,tmp)
    }
    
    #table(data_all$Date,useNA = 'always')
    
    
    data_all <- data_all %>% filter(Panel_ID %in% bed)
    
    data_all <- data_all %>%
        join(select(map[,c('min2','标准通用名','标准商品名','标准规格',
                                  '标准剂型','Pack_number','标准生产厂家')]),
             data_all$Product==map$min2, 'left') %>%
        join(select(universe[,c('Province','City','Panel_ID')]),
             data_all$Panel_ID==map$Panel_ID, 'left') %>%
        drop_dup_cols()
    
    data_all <- data_all %>%
        mutate(f_sales = ifelse(data_match$f_units<=0,
                                           0,
                                           data_match$f_sales))
    data_all <- data_all %>%
        mutate(f_sales = ifelse(data_match$f_sales<=0,
                                           0,
                                           data_match$f_sales))
    
    data_all <- data_all %>%
        mutate(f_units = ifelse(data_match$f_sales<=0,
                                           0,
                                           data_match$f_units))
    data_all <- data_all %>%
        mutate(f_units = ifelse(data_match$f_units<=0,
                                           0,
                                           data_match$f_units))
    
    
    
    data_all$units_r <- round(data_all$f_units)
    data_all$factor_unit <- data_all$units_r/data_all$f_units
    
    data_all <- data_all %>%
        mutate(factor_unit = 
                   ifelse(isNull(data_all$factor_unit), 0,
                          data_all$factor_unit))

    data_all$factor_unit[is.na(data_round$factor_unit)] <- 0
    
    data_round$sales_r <- round(data_round$f_sales*data_round$factor_unit,2)
    data_round$sales_r[data_round$units_r <=0] <- 0
    data_round$units_r[data_round$sales_r <=0] <- 0
    data_round$sales_r[data_round$sales_r <=0] <- 0
    data_round$units_r[data_round$units_r <=0] <- 0
    
    
    data_round$f_sales <- data_round$sales_r
    data_round$f_units <- data_round$units_r
    
    table(data_round$Date)
    print(sum(data_round$f_sales))
    
    
    data_format <- data_round %>% 
        group_by(Market=mkt,Date,Province,City=City.y,通用名=标准通用名,
                 商品名=标准商品名,剂型=标准剂型,规格=标准规格,
                 包装数=Pack_number,生产企业=标准生产厂家) %>%
        summarise(金额 = sum(f_sales,na.rm = T),
                    数量 = sum(f_units,na.rm = T))
    
    colSums(is.na(data_format))
    
    
    write.xlsx(data_format,paste0('E:/MAX/Tide/UPDATE/',c_month,'/MAX_'
                                  ,mkt,c_month,'.xlsx'))
}



