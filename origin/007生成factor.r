require(openxlsx)

require(stringi)
require(data.table)
require(RODBC)
require(sqldf)
require(tidyverse)

universe <- 
  read.xlsx('E:/MAX/Tide/MODEL/凯纷/000ref/universe.xlsx')

bed100 <- universe$Panel_ID[universe$BEDSIZE > 99]


spotfire_out <- fread(
  "L:/MAX result/Tide/1906/MAX_凯纷_20000_wo_ot_18.csv",
  stringsAsFactors = F)# %>% filter(substr(Date,4,4) %in% c('7'))

table(spotfire_out$Date)

names(spotfire_out)[5:6] <- c('Predict_Sales', 'Predict_Units')
panel <- read.xlsx('E:/MAX/Tide/MODEL/凯纷/010Panel_data/凯纷_Panel_2018.xlsx') 

panel1 <- inner_join(panel[panel$HOSP_ID %in% 
                             universe$Panel_ID[universe$PANEL %in% 1],], 
                     universe[, c('Panel_ID', 'Province', 'City')], 
                     by = c('HOSP_ID' = 'Panel_ID')) %>% group_by(City) %>%
  summarise(panel_sales = sum(Sales, na.rm = T))

rf_out <- read.xlsx('E:/MAX/Tide/MODEL/凯纷/051RF/2分之1_rf.xlsx')
rf_out2 <- rf_out %>% 
  left_join(universe[, c('Panel_ID', 'Province', 'City')], 
            by = c('PHA.ID' = 'Panel_ID')) %>% 
  filter(!PHA.ID %in% universe$Panel_ID[universe$PANEL %in% 1] )
rf_out3 <- group_by(rf_out2, City) %>%
  summarise(sales = sum(final_sales, na.rm = T))

spotfire_out1 <- spotfire_out %>% 
  left_join(universe[, c('Panel_ID', 'Province')], 
            by = c('Panel_ID')) %>%
  filter(Panel_ID %in% bed100 & !(Panel_ID %in% universe$Panel_ID[universe$PANEL %in% 1]))%>%
  group_by(City) %>% summarise(sales = sum(Predict_Sales, na.rm = T))

factor_city <- left_join(spotfire_out1,rf_out3,by='City')  %>%
  mutate(factor = sales.y/sales.x)

factor_city1 <- left_join(data.frame(City = unique(universe$City),
                                     stringsAsFactors = F), factor_city,
                          by = 'City')
summary(factor_city1$factor)
# factor_city1$factor <- 
#   ifelse(factor_city1$factor > 2, 2, factor_city1$factor)
factor_city1$factor[is.na(factor_city1$factor)] <- 1
summary(factor_city1$factor)
write.csv(factor_city1[,c('City', 'factor')], 
          'E:/MAX/Tide/MODEL/凯纷/050factor/凯纷_factor&city_full_1.csv',
          row.names = F)

