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

cal_seed_with_gr <- function(df, y, years,all_gr_index){
    
    
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
    
    total_gr = 1
    
    for(i in all_gr_index) {
        df[[i]] <- ifelse((df$min_index > i) |
                              (df$max_index < i), 1, df[[i]])
        df[[i]] <- ifelse(df$Year > y, df[[i]] ^ (-1), df[[i]])
        # df$Sales <- df$Sales * df[[i]]
        # df$Units <- df$Units * df[[i]]
        total_gr = total_gr * df[[i]]
    }
    df$Sales <- df$Sales * min(total_gr,2)
    df$Units <- df$Units * min(total_gr,2)
    
    df$Year <- y
    #print(head(df))
    return(df)
    
}
