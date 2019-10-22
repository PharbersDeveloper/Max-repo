require(openxlsx)

cal_excel_data_to_parquet <- function(path, tab, dest) {
    map <- read.xlsx(path, sheet = tab)
    mapDf <- createDataFrame(map)
    write.parquet(mapDf, dest)
}

cal_excel_large_data_to_parquet <- function(path, tab, dest, step = 10000L) {
    map <- read.xlsx(path, sheet = tab)
    d <- dim(map)
    print(d)
    round <- d[1] / step
    rleft <- d[1] %% step
    index <- 0
    while (index < floor(round)) {
        print("round")
        print(index)
        print(step * index + 1)
        print(step * index + step)
        tmp <- map[step * index + 1 : step * index + step, ]
        tmpDf <- createDataFrame(tmp)
        write.parquet(tmpDf, dest, mode = "append")
        index <- index + 1
    }
    tmpDf <- createDataFrame(map[step * round : step * round + rleft, ])
    write.parquet(tmpDf, dest, mode = "append")
    print("end")
}