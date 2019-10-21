
cal_data_adding_for_J <- function(uni_2019_path) {
    uni <- read.df(uni_2019_path, "parquet")
    uni <- data_add_uni_filter(uni)
    print(head(uni))
}

# 去重
data_add_uni_filter <- function(df) {
    df <- distinct(
            select(filter(df,  df$重复 == 0), 
                   "新版ID", "City", "City.Tier.2010")
            )
    
    df <- mutate(df,
                 City = ifelse(contains(df$新版ID, c("PHA0021658")), "大连市", "")
                )
    
    df <- mutate(df,
                 City = ifelse(contains(df$City.Tier.2010, c("PHA0021658")), "3", "")
                )
    
    return(df)
}
