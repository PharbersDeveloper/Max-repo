require(readxl)
require(data.table)

cal_excel_data_to_parquet <- function(path, tab, dest, start_row = 1) {
    map <- read_excel(path, sheet = tab, skip = start_row - 1,
                      .name_repair = "universal")
    mapDf <- createDataFrame(map)
    write.parquet(mapDf, dest)
}

cal_large_data_frame_2_spark <- function(map, dest, step = 10000L) {
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
        tmp <- map[(step * index + 1) : (step * index + step), ]
        tmpDf <- createDataFrame(tmp)
        write.parquet(tmpDf, dest, mode = "append")
        index <- index + 1
    }
    print(step * floor(round) + 1)
    print(step * floor(round) + rleft)
    tmpDf2 <- createDataFrame(map[(step * floor(round) + 1) : (step * floor(round) + rleft), ])
    print(SparkR::count(tmpDf2))
    write.parquet(tmpDf2, dest, mode = "append")
    print("end")   
}

cal_excel_large_data_to_parquet <- function(path, tab, dest, step = 10000L,
                                            start_row = 1) {
    map <- read_excel(path, sheet = tab, skip = start_row - 1,
                      .name_repair = "universal")
    cal_large_data_frame_2_spark(map, dest, step)
}

cal_csv_large_data_to_parquet <- function(path, dest, step = 10000L) {
    map <- fread(path, stringsAsFactors = F)
    cal_large_data_frame_2_spark(map, dest, step)
}
