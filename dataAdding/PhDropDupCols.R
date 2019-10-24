


drop_dup_cols <- function(df){
    tmp_col_name <- paste0(rep('a',max(nchar(names(df)))+1),collapse = '')
    names(df)[duplicated(names(df))] <- tmp_col_name
    df <- df %>% drop(tmp_col_name)
    return(df)
}
