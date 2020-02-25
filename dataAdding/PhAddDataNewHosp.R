
library(BPRSparkCalCommon)

add_data_new_hosp <- function(raw_data_adding, original_range){
    
    #raw_data_adding <- read.df(raw_data_adding_path, "parquet")

    # 1. 得到年
    #original_range <- distinct(select(seed, "Year", "Month", "PHA"))
    ws <- windowOrderBy("Year")
    years <- arrange(
        select(agg(groupBy(original_range, "Year"), tmp = lit(1)), "Year"),
        "Year"
    )
    years <- as.data.frame(years)[,"Year"]
    print(years)
    
    new_hospital <- 
        distinct(
            original_range[original_range$Year %in% max(years),'PHA']
        ) %>% 
        except(
            distinct(
                original_range[!(original_range$Year %in% max(years)),'PHA']
            )
        )
    
    new_hospital <- collect(new_hospital)
    new_hospital <- new_hospital$PHA
    print("以下是最新一年出现的医院")
    print(new_hospital)
    
    missing_months <- distinct(
        original_range[!(original_range$Year %in% max(years)),'Month']
    ) %>% 
        except(
            distinct(
                original_range[original_range$Year %in% max(years),'Month']
            )
        )
    
    number_of_existing_months <- 12 - nrow(missing_months)
    
    group_cols <- setdiff(names(raw_data_adding), 
                          c('Month','Sales','Units','季度',
                            "sales_value__rmb_",
                            "total_units",
                            "counting_units",
                            "year_month"))
   
    adding_data_new <- raw_data_adding %>% 
        filter((raw_data_adding$add_flag == 1) & 
                   (raw_data_adding$PHA %in% new_hospital))
   
    function_params <- list(adding_data_new)
    for (i in 1:length(group_cols)) {
        function_params[[i+1]] <- group_cols[i]
    }
    

    adding_data_new <- 
        do.call(SparkR::group_by, function_params) %>%
        SparkR::agg(Sales="sum",
            Units="sum") 
    
    adding_data_new <- adding_data_new %>% 
        mutate(Sales=adding_data_new$`sum(Sales)`/lit(number_of_existing_months),
               Units=adding_data_new$`sum(Units)`/lit(number_of_existing_months))
    
    adding_data_new <- adding_data_new %>%
        crossJoin(missing_months)
    
    #adding_data_new[['季度']] <- NA
    same_names <- intersect(names(raw_data_adding), names(adding_data_new))
    raw_data_adding <- rbind(raw_data_adding[,same_names], 
                             adding_data_new[,same_names])
    
    return(list(raw_data_adding, new_hospital))
}

