
add_data <- function(seed){
    original_range <- seed %>% select('Year','Month','PHA') %>% distinct()
    
    original_range_year <- distinct(original_range[,"Year"])
    years <- collect(original_range_year)
    
    years <- sort(years$Year)

    all_gr_index <- which(startsWith(names(seed),
                                     paste0('GR')))
    empty <- 0
    for(y in years){
        
        seed_range <- cal_time_range(original_range, y)
        
        seed_for_adding <- get_seed_data(seed,seed_range,y)
        
        seed_for_adding <- 
            cal_seed_with_gr(seed_for_adding, y, years,all_gr_index)
        
        if(empty == 0){
            adding_data <- seed_for_adding
        }else{
            adding_data <- rbind(adding_data, seed_for_adding)
        }
        
        empty <- empty + 1
    }
    
    return(list(adding_data,original_range))
    
}

cal_time_diff <- function(df, y){
    df <- df %>%
        mutate(
            time_diff = df$Year - y,
            weight = ifelse(
                df$Year > y,
                df$Year - y - 0.5,
                df$Year * (-1) +y
            )
        )
    return(df)
}

cal_time_range <- function(original_range, y){
    current_range_pha_month <-
        original_range[original_range$Year %in% y,
                       c('Month', 'PHA')] %>% distinct()
    current_range_month <- current_range_pha_month[,'Month'] %>% distinct()
    
    other_years_range <- 
        original_range[!(original_range$Year %in% y),]
    
    other_years_range <- other_years_range %>%
        join(current_range_month, 
             other_years_range$Month==current_range_month$Month,
             'inner') %>%
        drop_dup_cols()
    
    other_years_range <- other_years_range %>%
        join(current_range_pha_month, 
             other_years_range$Month==current_range_pha_month$Month &
                 other_years_range$PHA==current_range_pha_month$PHA,
             'left_anti') %>%
        drop_dup_cols()
    
    other_years_range <- other_years_range %>%
        cal_time_diff(y)
    
    seed_range <- other_years_range %>% 
        arrange(other_years_range$weight) %>%
        group_by('PHA','Month') %>%
        agg(Year = first(other_years_range$Year))
    
    return(seed_range)
}



