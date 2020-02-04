

remove_nega <- function(df, sales_unit){
    
    for(i in sales_unit){
        df <- filter(df, df[[i]]>0)
    }
    
    return(df)
}
