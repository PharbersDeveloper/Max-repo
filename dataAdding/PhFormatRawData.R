
format_raw_data <- function(df, std_names, cpa_gyc = T){
    names(df)[names(df) %in% c('数量（支/片）', '最小制剂单位数量',
                               'total_units','SALES_QTY')] <- 'Units'
    names(df)[names(df) %in% c('金额（元）', '金额', 'sales_value__rmb_',
                               'SALES_VALUE')] <- 
        'Sales'
    names(df)[names(df) %in% c('Yearmonth','YM', 'Date')] <- 'year_month'
    names(df)[names(df) %in% c('年', '年份','YEAR')] <- 'Year'
    names(df)[names(df) %in% c('月', '月份','MONTH')] <- 'Month'
    names(df)[names(df) %in% c('医院编码', 'BI_Code','HOSP_CODE')] <- 'ID'
    names(df)[names(df) %in% c('通用名', '药品名称', 'molecule_name',
                               'MOLE_NAME')] <- 
        'Molecule'
    names(df)[names(df) %in% c('商品名', '药品商品名','product_name',
                               'PRODUCT_NAME')] <- 'Brand'
    names(df)[names(df) %in% c('规格','pack_description',
                               'SPEC')] <- 'Specifications'
    names(df)[names(df) %in% c('剂型','formulation_name',
                               'DOSAGE')] <- 'Form'
    names(df)[names(df) %in% c('包装数量', '包装规格',
                               'PACK_QTY')] <- 'Pack_Number'
    names(df)[names(df) %in% c('生产企业','company_name',
                               'MANUFACTURER_NAME')] <- 'Manufacturer'
    names(df)[names(df) %in% c('省份', '省','省/自治区/直辖市',
                               'province_name','PROVINCE_NAME')] <- 'Province'
    names(df)[names(df) %in% c('城市','city_name','CITY_NAME')] <- 'City'
    names(df)[names(df) %in% c('PHA_ID_x','PHA_ID')] <- 'PHA'
    
    
    if(cpa_gyc){
        coltypes(df)[which(names(df) %in%
                               c("ID"))] <- "character"
        df$ID = ifelse(length(df$ID) < 7, lpad(df$ID, 6, "0"),
                       lpad(df$ID, 7, "0"))
    }
    
    if(('year_month' %in% names(df))){
        coltypes(df)[which(names(df) %in%
                               c("year_month"))] <- "integer"
    }
    
    
    
    
    if(!('Month' %in% names(df))){
        df <- mutate(df,
                           Month = df$year_month %% 100)
    }
    
    if(!('Pack_Number' %in% names(df))){
        df <- mutate(df,
                     Pack_Number = lit(0))
    }
    
    if(!('Year' %in% names(df))){
        df <- mutate(df,
                     Year = (df$year_month - df$Month) / 100)
    }
    
    
    
    # coltypes(df)[which(names(df) %in%
    #                              c("ID"))] <- "integer"
    coltypes(df)[which(names(df) %in%
                                 c("Year"))] <- "integer"
    coltypes(df)[which(names(df) %in%
                                 c("Month"))] <- "integer"
    
    
    
    df_m <- df[,std_names]
    
    return(df_m)
}
