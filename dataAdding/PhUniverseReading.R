read_universe <- function(uni_path) {
    uni <- read.df(uni_path, "parquet")
    
    uni <- uni %>%
        rename(City_Tier_2010 = uni$City_Tier,
               PHA = uni$Panel_ID)
    coltypes(uni)[which(names(uni) %in%
                            c("City_Tier_2010"))] <- "character"
    
    return(uni)
}
