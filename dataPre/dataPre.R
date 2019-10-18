require(openxlsx)

cal_excel_data_to_parquet <- function(path, tab) {
    map <- read.xlsx(path, sheet = tab)
    mapDf <- createDataFrame(map)
    write.parquet(mapDf, "hdfs://192.168.100.137:9000//Map-repo/2019年Universe更新维护1.0_190403/Universe2019")
}