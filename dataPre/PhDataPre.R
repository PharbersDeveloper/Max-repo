
source("dataPre/dataPre.R")

cal_J_data_pre <- function() {
    # 0. 数据准备
    # 0.1 Universe 文件
    cal_excel_data_to_parquet(
        "/Users/alfredyang/Desktop/code/pharbers/Max-repo/tmp/2019年Universe更新维护1.0_190403.xlsx", 
        "Universe2019", 
        "hdfs://192.168.100.137:9000//Map-repo/2019年Universe更新维护1.0_190403/Universe2019")
    
    # 0.2 Mapping 文件
    cal_excel_data_to_parquet(
        "/Users/alfredyang/Desktop/code/pharbers/Max-repo/tmp/2019年Universe更新维护1.0_190403.xlsx", 
        "Mapping", 
        "hdfs://192.168.100.137:9000//Map-repo/2019年Universe更新维护1.0_190403/Mapping")
    
    # 0.3 CPA_VS_GYC_VS_PHA_VS_HH_0418 # Sheet1
    cal_excel_data_to_parquet(
        "/Users/alfredyang/Desktop/code/pharbers/Max-repo/tmp/CPA_VS_GYC_VS_PHA_VS_HH_0418.xlsx",
        "Sheet1", 
        "hdfs://192.168.100.137:9000//Map-repo/CPA_VS_GYC_VS_PHA_VS_HH_0418")
    
    # 0.3 190814泰德-1701-1906检索 # 1701-1906
    cal_excel_large_data_to_parquet(
        "/Users/alfredyang/Desktop/code/pharbers/Max-repo/tmp/190814泰德-1701-1906检索.xlsx",
        "1701-1906数据", 
        "hdfs://192.168.100.137:9000//Map-repo/190814泰德-1701-1906检索/1701-1906")
    
    # 0.4 universe # 1701-1906
    cal_excel_data_to_parquet(
        "/Users/alfredyang/Desktop/code/pharbers/Max-repo/tmp/universe.xlsx",
        "Sheet1", 
        "hdfs://192.168.100.137:9000//Map-repo/universe")
    
    # 0.5 通用名企业层面集中度_pb.xlsx # Sheet 1
    cal_excel_data_to_parquet(
        "/Users/alfredyang/Desktop/code/pharbers/Max-repo/tmp/通用名企业层面集中度_pb.xlsx",
        "Sheet 1", 
        "hdfs://192.168.100.137:9000//Map-repo/通用名企业层面集中度_pb")
    
    # 0.6 泰德产品匹配表.xlsx # Sheet 1
    cal_excel_data_to_parquet(
        "/Users/alfredyang/Desktop/code/pharbers/Max-repo/tmp/泰德产品匹配表.xlsx",
        "Sheet 1", 
        "hdfs://192.168.100.137:9000//Map-repo/泰德产品匹配表")
}
