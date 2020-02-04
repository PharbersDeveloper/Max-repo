

read_raw_data <- function(raw_data_path, map_cpa_pha) {
  raw_data <- read.df(raw_data_path, "parquet")

  
 

  raw_data <- join(
    raw_data, map_cpa_pha[, c("PHA_ID_x", "BI_hospital_code")],
    raw_data$BI_Code == map_cpa_pha$BI_hospital_code, "left"
  )

  raw_data <- format_raw_data(raw_data)
  


  return(raw_data)
}