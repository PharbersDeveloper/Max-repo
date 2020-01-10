

read_uni_ot <- function(uni_ot_path){
    universe_ot <- read_universe(uni_ot_path)
    
    universe_ot <- universe_ot %>%
        select('PHA','Est_DrugIncome_RMB',
               'PANEL','Seg', 'BEDSIZE')
    
    return(universe_ot)
}