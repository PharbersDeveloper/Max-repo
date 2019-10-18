require(openxlsx)

cal_excel_data_to_parquet <- function(path, tab, dest) {
    map <- read.xlsx(path, sheet = tab)
    mapDf <- createDataFrame(map)
    write.parquet(mapDf, dest)
}