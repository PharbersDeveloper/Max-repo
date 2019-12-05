
map_cpa_pha <- function(
                        map_cpa_pha_path) {
  map_cpa_pha_v1 <- read.df(map_cpa_pha_path, "parquet")
  map_cpa_pha_v1 <- map_cpa_pha_v1[, c('PHA_ID_x', 'BI_hospital_code')] %>%
      distinct()
  return(map_cpa_pha_v1)
}

