
combind_data <- function(raw_data, adding_data){
    raw_data[['add_flag']] <- 0
    adding_data[['add_flag']] <- 1
    return(rbind(raw_data,adding_data[,names(raw_data)]))
    
}
