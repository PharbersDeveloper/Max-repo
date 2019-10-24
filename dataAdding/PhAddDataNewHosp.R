


add_data_new_hosp <- function(raw_data_adding, original_range){
    
    original_range_year <- distinct(original_range[,"Year"])
    years <- collect(original_range_year)
    
    years <- sort(years$Year)
    
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
                          c('Month','Sales','Units','季度'))
    
    adding_data_new <- raw_data_adding %>% 
        filter(raw_data_adding$add_flag==1 & 
                   raw_data_adding$新版ID %in% new_hospital)
    adding_data_new <- adding_data_new %>%
        group_by(group_cols) %>%
        agg(Sales=sum(adding_data_new$Sales)/number_of_existing_months,
            Units=sum(adding_data_new$Units)/number_of_existing_months)
    
    adding_data_new <- adding_data_new %>%
        crossJoin(adding_data_new, missing_months)
    
    return(adding_data_new)
    
}