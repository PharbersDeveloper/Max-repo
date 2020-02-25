

multi_struct_fileds  <- function(array_colnames, array_types){
    struct_type <- paste(array_colnames[1], array_types[1])
    if(length(array_colnames) == 1){
        return(structType(truct_type))
    }
    for(i in 2:length(array_colnames)){
        tmp <- paste(array_colnames[i], array_types[i])
        struct_type <- paste(struct_type, tmp, sep = ', ')
    }
    return(structType(struct_type))
}
