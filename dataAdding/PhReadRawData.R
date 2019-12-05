

read_raw_data <- function(raw_data_path, map_cpa_pha) {
  raw_data <- read.df(raw_data_path, "parquet")

  coltypes(raw_data)[which(names(raw_data) %in%
    c("year_month"))] <- "integer"
 

  raw_data <- join(
    raw_data, map_cpa_pha[, c("PHA_ID_x", "BI_hospital_code")],
    raw_data$BI_Code == map_cpa_pha$BI_hospital_code, "left"
  )

  raw_data <- mutate(raw_data,
    Month = raw_data$year_month %% 100,
    Sales = raw_data$sales_value__rmb_,
    Units = raw_data$total_units,
    std_mole = raw_data$molecule_name,
    PHA = raw_data$PHA_ID_x,
    医院编码 = raw_data$BI_Code
  )
  raw_data <- raw_data %>%
    mutate(Year = (raw_data$year_month - raw_data$Month) / 100)


  return(raw_data)
}