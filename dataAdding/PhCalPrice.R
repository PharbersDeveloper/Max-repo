


cal_price <- function(raw_data) {
    
    price <- group_by(raw_data, 'min2', 'year_month', 'City_Tier_2010') %>%
        agg(Price = sum(raw_data$Sales)/sum(raw_data$Units))
    return(price)
}