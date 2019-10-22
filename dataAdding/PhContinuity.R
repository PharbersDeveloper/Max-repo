
con.count.all1<-con.count.all[,1:3]%>%unique()%>%
    spread(年,n,fill=0)%>%
    mutate(`总出现次数`=`2017`+`2018`+`2019`)%>%
    mutate(min=pmin(`2017`,`2018`,`2019`),
           max=pmax(`2017`,`2018`,`2019`))
write.xlsx(con.count.all1,
           'E:/MAX/Tide/Global/Tide项目医院连续性表现_医院层面.xlsx')
write.xlsx(con.count.all,
           'E:/MAX/Tide/Global/医院两年半连续性表现.xlsx')

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
        select(con, "新版ID","Year","count")
    )
    
    
    con <- partitionBy(con, con$Year)
    
    return(raw_data)
    
}