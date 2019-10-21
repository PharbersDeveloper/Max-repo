
cal_data_adding_for_J <- function(uni_2019_path) {
    uni <- read.df(uni_2019_path, "parquet")
    uni <- data_add_uni_filter(uni)
    print(head(uni))
}

# 去重
data_add_uni_filter <- function(df) {
    df <- distinct(
            select(filter(df, df$重复 == 0), 
                   c("新版ID", "City", "City_Tier_2010")
                  )
            )
    
    df <- mutate(df,
                 City = ifelse(contains(df$新版ID, c("PHA0021658")), "大连市", df$City)
                )
    
    df <- mutate(df,
                 City_Tier_2010 = ifelse(contains(df$City_Tier_2010, c("PHA0021658")), "3", df$City_Tier_2010)
                )
    
    return(df)
}
