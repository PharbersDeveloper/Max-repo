

read_raw_data <- function(raw_data_path, map_cpa_pha){
    raw_data <- read.df(raw_data_path, 'parquet')
    
    coltypes(raw_data)[which(names(raw_data) %in% 
                                 c('医院编码', '年', '月'))] <- 'integer'
    
    raw_data <- join(raw_data,map_cpa_pha, 
                     raw_data$医院编码==map_cpa_pha$CPA, 'left')
    
    raw_data <- rename(raw_data, Year=raw_data$年, Month = raw_data$月,
                       Sales=raw_data$金额, Units=raw_data$数量)
    return(raw_data)
}