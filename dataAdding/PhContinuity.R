
cal_continuity <- function(raw_data){
    
    con <- distinct(select(raw_data,'Year','Month','新版ID'))
    
    con <- count(groupBy(con, '新版ID', 'Year'))
    
    con_whole_year <- agg(group_by(con,'新版ID'),
                         MAX=max(con$count),
                         MIN=min(con$count))
    
    con <- join(con, con_whole_year, con$新版ID==con_whole_year$新版ID, 'left')
    
    colnames(con)[which(names(con) %in% c('新版ID'))[-1]] <- 'tmp'
    
    con <- mutate(
        con, 
        MAX = ifelse(isNull(con$MAX), 0, con$MAX),
        MIN = ifelse(isNull(con$MIN), 0, con$MIN)
    )
    
    distribution <- count(group_by(distinct(select(con,'MAX','MIN','新版ID')),
                                   'MAX','MIN'))
    
    con <- distinct(
        select(con, "新版ID", "Year", "count")
    )
 
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
    
    print(head(con))
    
    return(raw_data)
    
}