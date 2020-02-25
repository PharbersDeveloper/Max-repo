
map_cpa_pha <- function(
                        map_cpa_pha_path, universe) {
  map_cpa_pha_v1 <- read.df(map_cpa_pha_path, "parquet")
  
  map_cpa_pha_v1 <- filter(map_cpa_pha_v1, map_cpa_pha_v1$推荐版本 == 1)
  names(map_cpa_pha_v1)[names(map_cpa_pha_v1) %in% 'ID'] <- 'BI_hospital_code'
  names(map_cpa_pha_v1)[names(map_cpa_pha_v1) %in% 'PHA'] <- 'PHA_ID_x'
  
  map_cpa_pha_v1 <- map_cpa_pha_v1[, c('PHA_ID_x', 'BI_hospital_code')] %>%
      distinct()
  
  if(!all(c('Province','City') %in% names(map_cpa_pha_v1))){
      map_cpa_pha_v1 <- map_cpa_pha_v1 %>% drop(c('Province','City'))
      map_cpa_pha_v1 <- 
          join(map_cpa_pha_v1, select(universe,'PHA','Province','City'),
               map_cpa_pha_v1$PHA_ID_x == universe$PHA, 'left')
      map_cpa_pha_v1 <- map_cpa_pha_v1 %>% drop('PHA')
  }
  return(map_cpa_pha_v1)
}


