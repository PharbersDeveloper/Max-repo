
source("dataPre/dataPre.R")

cal_J_data_pre <- function() {
    # 0. 数据准备

    # 0.2 Mapping 文件
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/Global/医院匹配/Hospital_Code_PHA_final_2.xlsx", 
        "BI_to_PHA", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/MappingPha")
    
    
    # 0.3 190814泰德-1701-1906检索 # 1701-1906
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/UPDATE/1908/Hospital Data for Zytiga Market 201801-201908.xlsx",
        "Zytiga Broad wo discount", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Zytiga_Market_201801-201908",
        4)
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/UPDATE/1907/Hospital Data for Zytiga Market 201801-201907.xlsx",
        "Zytiga Broad wo discount", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Zytiga_Market_201801-201907",
        4)
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/UPDATE/1909/Hospital Data for Zytiga Market 201801-201909.xlsx",
        "Zytiga Broad wo discount", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Zytiga_Market_201801-201909",
        4)
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/UPDATE/1909/Hospital Data for Sustenna Market 201801-201909.xlsx",
        "Sustenna Sales Data", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Sustenna_Market_201801-201909",
        4)
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/UPDATE/1910/Hospital Data for Zytiga Market 201801-201910.xlsx",
        "Zytiga Broad wo discount", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Zytiga_Market_201801-201910",
        4)
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/UPDATE/1910/Hospital Data for Sustenna Market 201801-201910.xlsx",
        "Sustenna Sales Data", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Sustenna_Market_201801-201910",
        4)
    
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/UPDATE/1907/Hospital Data for Sustenna Market 201801-201907.xlsx",
        "Zytiga Broad wo discount", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Hospital_Data_for_Sustenna_Market_201801-201907",
        4)
    
    # 0.4 universe # 1701-1906
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/MODEL/Sustenna/000ref/universe.xlsx",
        "Sheet 1", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/universe_Sustenna")
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/MODEL/Zytiga/000ref/universe.xlsx",
        "Sheet 1", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/universe_Zytiga")
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/MODEL/Sustenna/030ot_input/universe_ot_Sustenna.xlsx",
        , 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/universe_ot_Sustenna")
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/MODEL/Zytiga/030ot_input/universe_ot.xlsx",
        , 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/universe_ot_Zytiga")
    
    
    # 0.5 factor
    cal_csv_to_parquet(
        "Y:/MAX/Janssen/MODEL/Sustenna/050factor/Sustenna_factor&city_full.csv", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/factor_Sustenna")
    
    cal_csv_to_parquet(
        "Y:/MAX/Janssen/MODEL/Zytiga/050factor/Zytiga_factor&city_full.csv", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/factor_Zytiga")
    
    # 0.6 泰德产品匹配表.xlsx # Sheet 1
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/Global/Product_matching_table_packid_v2.xlsx",
        "Sheet 1", 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/产品匹配表")
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/Global/各个分子分城市等级1819增长率文件.xlsx",
        , 
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/gr_with_id")
    
    
    cal_csv_large_data_to_parquet(
        "W:/MAX result/Janssen/1909/Zytiga_Factorized_Units&Sales_WITH_OT1909.csv",
        "hdfs://192.168.100.137:8020//common/projects/max/Janssen/Zytiga_hosp_MAX_result_1909")
    




}
