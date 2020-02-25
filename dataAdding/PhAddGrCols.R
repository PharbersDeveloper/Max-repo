


add_gr_cols <- function(df, years){
    for(i in 1:(length(years)-1)){
        df[[paste0('GR',substr(years[i], 3, 4),substr(years[i+1], 3, 4))]] <- 
            df[[paste0("Year_", years[i+1])]]/df[[paste0("Year_", years[i])]]
    }
    return(df)
}