

combind_max_data <- function(paths, bed, universe, prod_mapping, 
                             if_dosage_unit = T){
    for(i in 1:length(paths)){
        p = paths[i]
        
        
        tmp <- read.df(p, 'parquet')
        
        names(tmp)[names(tmp) %in% 'Panel_ID'] <- 'PHA'
        names(tmp)[names(tmp) %in% 'f_sales'] <- 'Predict_Sales'
        names(tmp)[names(tmp) %in% 'f_units'] <- 'Predict_Unit'
        names(tmp)[names(tmp) %in% 'Prod_Name'] <- 'Product'
        
        tmp <- tmp %>%
            join(bed, tmp$PHA == bed$PHA, 'inner') %>%
            drop_dup_cols()
        
        if(!all(c('Province', 'City') %in% names(tmp))){
            tmp <- tmp %>%
                drop(c('City', 'PHA'))
            universe_city <- universe %>% SparkR::select('PHA', 'Province', 'City')
            tmp <- tmp %>%
                join(universe_city, 
                     tmp$PHA == universe_city$PHA, 'left') %>%
                drop_dup_cols()
        }
        
        
        tmp <- mapping_prod(tmp, prod_mapping)
        
        ##杨森要换算成片
        if(!if_dosage_unit){
            print('盒换算片')
            tmp$Predict_Unit <- tmp$Predict_Unit * tmp$包装数量
        }
        
        tmp <- unit_based_round(tmp)
        
        tmp <- tmp %>%
            group_by('Date', 'Province', 'City', '通用名',
                     '商品名', '剂型', '规格', '包装数量', '生产企业',
                     'ATC3') %>%
            agg(金额 = sum(tmp$Predict_Sales),
                数量 = sum(tmp$Predict_Unit))
        names(tmp)[names(tmp)=='Province'] <- '省份'
        names(tmp)[names(tmp)=='City'] <- '城市'
        
        
        tmp <- tmp[,c(1:3,11:12,4:10)]
        
        #tmp <- tmp %>% mutate(Market = lit(gsub('.*/|_.*','',p)))
        
        if(i == 1){
            max_data <- tmp
        }else{
            max_data <- rbind(max_data, tmp)
        }
        
    }
    return(max_data)
}