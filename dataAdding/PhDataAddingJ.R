
cal_data_adding_for_J <- function(
    uni_2019_path
) {
    map <- read.df(uni_2019_path, "parquet")
    print(head(map))
}