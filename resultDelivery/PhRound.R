
unit_based_round <- function(df){
    
    unit_r <- bround(df$f_units)
    f <- unit_r/df$f_units
    f <- ifelse(isNull(f), 0, f)
    
    df <- df %>% mutate(f = f)
    df$f_units <- unit_r
    df$f_sales <- bround(df$f_sales * df$f, 2)
    
    
    
    df$f_sales <- ifelse(df$f_units <= 0, 0,
                         df$f_sales)
    
    df$f_sales <- ifelse(df$f_sales <= 0, 0,
                         df$f_sales)
    
    
    df$f_units <- ifelse(df$f_sales <= 0, 0,
                         df$f_units)
    
    df$f_units <- ifelse(df$f_units <= 0, 0,
                         df$f_units)
    
    return(
        df
    )
}






