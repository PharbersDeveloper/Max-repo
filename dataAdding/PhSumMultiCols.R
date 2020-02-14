sum_multi_columns <- function(df, array_names){
    s <- df[[array_names[1]]]
    for(i in 2:length(array_names)){
        s=s+df[[array_names[i]]]
    }
    return(s)
}