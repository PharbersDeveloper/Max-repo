
cal_continuity <- function(raw_data){
    
    con <- distinct(select(raw_data,'Year','Month','新版ID'))
    
    con <- count(groupBy(con, '新版ID', 'Year'))
    
    con_whole_year <- agg(group_by(con,'新版ID'),
                         MAX=max(con$count),
                         MIN=min(con$count))
    
    #drop_dup_cols()的定义在dataAdding/PhDropDupCols.R
    con_dis <- join(con, con_whole_year, 
                    con$新版ID==con_whole_year$新版ID, 'left') %>%
        drop_dup_cols()
    
    
    con_dis <- mutate(
        con_dis, 
        MAX = ifelse(isNull(con_dis$MAX), 0, con_dis$MAX),
        MIN = ifelse(isNull(con_dis$MIN), 0, con_dis$MIN)
    )
    
    distribution <-
        count(group_by(distinct(select(
            con_dis, 'MAX', 'MIN', '新版ID'
        )),
        'MAX', 'MIN'))
    
 
    con <- repartition(con, 2L, con$新版ID)
    
    printSchema(con)
    con_schema <- structType(
        structField("新版ID", "string"),
        structField("Year_2017", "double"),
        structField("Year_2018", "double"),
        structField("Year_2019", "double"),
        structField("Count", "double")
    )
    
    con <- 
        dapply(con,
            function(x) {
                library("tidyverse")
                x <- unique(x)
                x <- spread(x, "Year", "count", fill = 0)
                return(x)
                
            }, con_schema
        )
    
    con <- con %>% 
        mutate(total=con$Year_2017+con$Year_2018+con$Year_2019,
               PHA = con$新版ID, 
               min = 
                   min_max(con, 
                           c('Year_2017','Year_2018','Year_2019'))[[1]],
               max = 
                   min_max(con, 
                           c('Year_2017','Year_2018','Year_2019'))[[2]]) %>% 
        drop(c('Count','新版ID'))
    
    print(head(con))
    
    return(list(con_dis,con))
    
}




# min_max <- function(df){
#     min <- ifelse(df$Year_2017 > df$Year_2018, df$Year_2018, df$Year_2017)
#     min <- ifelse(min > df$Year_2019, df$Year_2019, min)
#     
#     max <- ifelse(df$Year_2017 < df$Year_2018, df$Year_2018, df$Year_2017)
#     max <- ifelse(max < df$Year_2019, df$Year_2019, max)
#    
#     return(list(min,max))
# }
