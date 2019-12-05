
source("dataPre/dataPre.R")

cal_J_data_pre <- function() {
    # 0. 数据准备

    # 0.2 Mapping 文件
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/Global/医院匹配/Hospital_Code_PHA_final_2.xlsx", 
        "BI_to_PHA", 
        "hdfs://192.168.100.137:9000//Map-repo/Janssen/MappingPha")
    
    
    # 0.3 190814泰德-1701-1906检索 # 1701-1906
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/UPDATE/1908/Hospital Data for Zytiga Market 201801-201908.xlsx",
        "Zytiga Broad wo discount", 
        "hdfs://192.168.100.137:9000//Map-repo/Janssen/Hospital_Data_for_Zytiga_Market_201801-201908",
        4)
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/UPDATE/1907/Hospital Data for Zytiga Market 201801-201907.xlsx",
        "Zytiga Broad wo discount", 
        "hdfs://192.168.100.137:9000//Map-repo/Janssen/Hospital_Data_for_Zytiga_Market_201801-201907",
        4)
    
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/UPDATE/1907/Hospital Data for Sustenna Market 201801-201907.xlsx",
        "Zytiga Broad wo discount", 
        "hdfs://192.168.100.137:9000//Map-repo/Janssen/Hospital_Data_for_Sustenna_Market_201801-201907",
        4)
    
    # 0.4 universe # 1701-1906
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/MODEL/Sustenna/000ref/universe.xlsx",
        "Sheet 1", 
        "hdfs://192.168.100.137:9000//Map-repo/Janssen/universe_sustenna")
    
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/MODEL/Zytiga/000ref/universe.xlsx",
        "Sheet 1", 
        "hdfs://192.168.100.137:9000//Map-repo/Janssen/universe_Zytiga")
    
    
    # 0.6 泰德产品匹配表.xlsx # Sheet 1
    cal_excel_data_to_parquet(
        "Y:/MAX/Janssen/Global/Product_matching_table_packid_v2.xlsx",
        "Sheet 1", 
        "hdfs://192.168.100.137:9000//Map-repo/Janssen/产品匹配表")
    




}
