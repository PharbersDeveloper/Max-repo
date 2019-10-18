require(openxlsx)
require(dplyr)
require(tidyr)
#require(sca)
require(readr)
require(stringr)
require(stringi)
# require(rPython)
require(data.table)
# variables for test

c_month <- '1907'

universe <- read.xlsx("E:/MAX/Tide/MODEL/凯纷/000ref/universe.xlsx")
bed <- universe$Panel_ID[universe$BEDSIZE >99]

prod_mapping <- read.xlsx("E:/MAX/Tide/Global/泰德产品匹配表.xlsx")

prod_mapping$Pack_ID <- stri_pad_left(prod_mapping$Pack_ID,7,'0')


#### MAX ####

for(mkt in c('凯纷','凯那','凯时','维格列汀片')){
  print(mkt)
  data_all <- NULL
  for(i in c_month){
    print(i)
    tmp <- fread(paste0("L:/MAX result/Tide/",i,"/",mkt,
                        "_Factorized_Units&Sales_WITH_OT",i, ".csv"))
    data_all <- rbind(data_all,tmp)
  }
  
  table(data_all$Date,useNA = 'always')
  
  data_match <- data_all %>% filter(Panel_ID %in% bed) %>%
    left_join(prod_mapping[,c('min2','标准通用名','标准商品名','标准规格',
                              '标准剂型','Pack_number','标准生产厂家')],
              by=c('Product'='min2')) %>%
    left_join(universe[,c('Province','City','Panel_ID')],by=c('Panel_ID'))
  
  nrow(data_match)==nrow(data_all)
  sum(is.na(data_match$标准通用名))
  
  names(data_match)
  
  
  data_match$f_sales[data_match$f_units <= 0] <- 0
  data_match$f_sales[data_match$f_sales <= 0] <- 0
  
  data_match$f_units[data_match$f_sales <= 0] <- 0
  data_match$f_units[data_match$f_units <= 0] <- 0
  
  
  print(sum(data_all$f_sales))
  print(sum(data_match$f_sales))
  
  data_round <- data_match
  data_round$units_r <- round(data_round$f_units)
  data_round$factor_unit <- data_round$units_r/data_round$f_units
  summary(data_round$f_units)
  data_round$factor_unit[is.na(data_round$factor_unit)] <- 0
  
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

#### 样本 ####

for(mkt in c('凯纷','凯那','凯时','维格列汀片')){
  print(mkt)
  panel <- NULL
  for(i in c_month){
    print(i)
    tmp <- read.xlsx(paste0("E:/MAX/Tide/UPDATE/",i,"/",
                            mkt,"_Panel_",i,".xlsx"))
    panel <- rbind(panel,tmp)
  }
  
panel_match <- panel %>%
  left_join(universe[,c('Province','City','Panel_ID')],by=c('HOSP_ID'='Panel_ID')) %>%
  left_join(prod_mapping[,c('标准通用名','标准商品名','标准规格','标准剂型',
                            'Pack_number','标准生产厂家','min2')],
            by=c('Prod_Name'='min2'))

nrow(panel)==nrow(panel_match)

colSums(is.na(panel_match))

names(panel_match)[names(panel_match) %in% 'ID'] <- 'CPA_ID'
names(panel_match)[names(panel_match) %in% '标准通用名'] <- '通用名'
names(panel_match)[names(panel_match) %in% '标准商品名'] <- '商品名'
names(panel_match)[names(panel_match) %in% '标准规格'] <- '规格'
names(panel_match)[names(panel_match) %in% '标准剂型'] <- '剂型'
names(panel_match)[names(panel_match) %in% 'Pack_number'] <- '包装数'
names(panel_match)[names(panel_match) %in% '标准生产厂家'] <- '生产企业'
names(panel_match)[names(panel_match) %in% 'DOI'] <- 'Market'
names(panel_match)[names(panel_match) %in% 'Sales'] <- '金额'
names(panel_match)[names(panel_match) %in% 'Units'] <- '数量'

panel_format <- panel_match[!is.na(panel_match$HOSP_ID),
                             c('Date','Province','CPA_ID','City','HOSP_ID',
                               '通用名','商品名','剂型','规格','包装数',
                               '生产企业','Market','金额','数量')]

print(sum(panel_format$金额))

colSums(is.na(panel_format))

write.xlsx(panel_format,paste0('E:/MAX/Tide/UPDATE/',c_month,'/样本_'
                               ,mkt,c_month,'.xlsx'))
}
