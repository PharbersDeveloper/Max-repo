min_max <- function(df,cols){
    
    # 类型从df变成column
    max <- df[[cols[1]]]
    min <- df[[cols[1]]]
    for(col in cols[-1]){
        max <- max_by_row(max,df[[col]])
        min <- min_by_row(min,df[[col]])
    }
    # 刚刚的column并入df中
    df <-  mutate(df, max=max,min=min)
    return(df)
}


max_by_row <- function(a,b){
    return(ifelse(a>b,a,b))
}

min_by_row <- function(a,b){
    return(ifelse(a<b,a,b))
}
