
cal_data_adding_for_J <- function(
    uni_2019_path
) {
    map <- read.parquet(uni_2019_path)
    print(head(map))
}