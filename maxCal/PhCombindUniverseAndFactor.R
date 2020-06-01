




get_uni_with_factor <- function(factor_path, universe){
    
    factor <- read.df(factor_path,
                      'parquet')
    if(!('factor' %in% names(factor))){
        names(factor)[names(factor) %in% 'factor_new'] <- 'factor'
    }
    
    factor <- factor %>% select('City', 'factor')
    
    universe <- universe %>%
        join(factor, universe$City == factor$City, 'left') %>%
        drop_dup_cols()
    
    universe$factor <- ifelse(isNull(universe$factor),
                              lit(1),
                              universe$factor)
    
    
    universe <- universe %>%
        filter(universe$PANEL == 0) %>% 
        select('Province','City','PHA', 'Est_DrugIncome_RMB', 'Seg',
               'BEDSIZE', 'PANEL','factor')
    
    return(universe)
    
}