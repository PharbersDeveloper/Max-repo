get_seed_data <- function(seed, seed_range, y){
    seed_for_adding <- seed[!(seed$Year %in% y),]
    
    seed_for_adding <- seed_for_adding %>%
        join(seed_range, seed_for_adding$Month==seed_range$Month &
                 seed_for_adding$PHA==seed_range$PHA &
                 seed_for_adding$Year==seed_range$Year, 'inner') %>%
        drop_dup_cols()
    
    
    seed_for_adding <- seed_for_adding %>%
        cal_time_diff(y)
    return(seed_for_adding)
}

cal_seed_with_gr <- function(df, y, years,all_gr_index, price_path){
    
    price <- read.df(price_path, 'parquet')
    
    base_index <- y-min(years)+min(all_gr_index)
    
    df$Sales_bk <- df$Sales
    
    df <- df %>%
        mutate(
            min_index = ifelse(df$Year < y, 
                               df$time_diff + 
                                   base_index, 
                               base_index),
            max_index = ifelse(df$Year < y, 
                               base_index - 1, 
                               df$time_diff +
                                   base_index - 1)
        )
    
    df$total_gr = lit(1)
    
    for(i in all_gr_index) {
        df[[i]] <- ifelse((df$min_index > i) |
                              (df$max_index < i), 1, df[[i]])
        df[[i]] <- ifelse(df$Year > y, df[[i]] ^ (-1), df[[i]])
        # df$Sales <- df$Sales * df[[i]]
        # df$Units <- df$Units * df[[i]]
        df$total_gr = df$total_gr * df[[i]]
    }
    df$final_gr <- min_by_row(df$total_gr, lit(2))
    
    df$Sales <- df$Sales * df$final_gr
    #df$Units <- df$Units * df$final_gr
    
    df$Year <- y
    
    df$year_month <- df$Year * 100 + df$Month
    
    df <- df %>% join(price, df$min2 == price$min2 & 
                          df$year_month == price$year_month &
                      df$CITYGROUP == price$City_Tier_2010) %>%
        drop_dup_cols()
    df$Units <- ifelse(df$Sales==0,0,df$Sales / df$Price)
    
    df$Units <- ifelse(isNull(df$Units),0,df$Units)
    
    #print(head(df))
    return(df)
    
}
