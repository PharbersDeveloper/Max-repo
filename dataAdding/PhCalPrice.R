


cal_price <- function(raw_data) {
    
    price <- group_by(raw_data, 'min2', 'year_month', 'City_Tier_2010') %>%
        agg(Price = sum(raw_data$Sales)/sum(raw_data$Units))
    
    price2 <- group_by(raw_data, 'min2', 'year_month') %>%
        agg(Price2 = sum(raw_data$Sales)/sum(raw_data$Units))
    
    price <- join(price, price2, price$min2 == price2$min2 &
                      price$year_month == price2$year_month, 'left') %>%
        drop_dup_cols()
    price$Price <- ifelse(isNull(price$Price), price$price2, price$Price)
    
    price$Price <- ifelse(isNull(price$Price), 0, price$Price)
    
    price <- drop(price, 'Price2')
    return(price)
}