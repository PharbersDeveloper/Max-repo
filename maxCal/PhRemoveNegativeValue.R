

remove_nega <- function(df, sales_unit){
    
    df <- mutate(df, positive = ifelse(df[[sales_unit[1]]]>0, 1, 0))
    
    for(i in sales_unit[-1]){
        df <- mutate(df, positive = ifelse(df[[i]]>0, 1, df$positive))
    }
    
    df <- filter(df, df$positive == 1)
    
    df <- df %>%
        drop('positive')
    
    return(df)
}
