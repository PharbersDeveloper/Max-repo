combind_raw_data <- function(paths, cpa_pha_mapping, dup_gyc=NULL){
    

    for(i in 1:length(paths)){
        p=paths[i]
        
        tmp <- read_raw_data(p, cpa_pha_mapping)
        
        if(i == 1){
            raw_data=tmp
        }else{
            raw_data <- raw_data %>% rbind(tmp)
        }
        
    }
    
    
    raw_data <- raw_data %>% filter(!(raw_data$ID %in% dup_gyc))

    return(raw_data)
}
