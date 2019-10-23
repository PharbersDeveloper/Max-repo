cal_growth <- function(raw_data, id_city){
    gr_raw_data <- join(raw_data, id_city,
                        raw_data$新版ID == id_city$新版ID, 'left') 
    colnames(gr_raw_data)[which(names(gr_raw_data) %in% c('新版ID'))[-1]] <- 
        'tmp'
    
    
    gr_raw_data <- mutate(gr_raw_data, 
                          City_Tier_2010=ifelse(
                              isNull(gr_raw_data$City_Tier_2010),
                              5,
                              gr_raw_data$City_Tier_2010
                          ))
    
    gr_raw_data <- rename(gr_raw_data, CITYGROUP = gr_raw_data$City_Tier_2010)
    
    
    gr <- gr_raw_data %>%
        group_by('Year', 'std_mole', 'CITYGROUP') %>% 
        agg(value=sum(gr_raw_data$Sales))
    
    
    gr <- repartition(gr, 2L, 
                      gr$std_mole, 
                      gr$CITYGROUP)
    
    con_schema <- structType(
        structField("std_mole", "string"),
        structField("CITYGROUP", "double"),
        structField("Year_2017", "double"),
        structField("Year_2018", "double"),
        structField("Year_2019", "double"),
        structField("Value", "double")
    )
    
    gr <- 
        dapply(gr,
               function(x) {
                   library("tidyverse")
                   x <- unique(x)
                   x <- spread(x, "Year", "Value", fill = 0)
                   return(x)
                   
               }, con_schema
        )
    gr <- gr %>%
        mutate(GR1718=gr$Year_2018/gr$Year_2017)
    
    
    gr <- modify_gr(gr, names(gr)[startsWith(names(gr),'GR')])
    
    gr_with_id <- gr_raw_data %>%
        select('新版ID', '医院编码', 'City', 'CITYGROUP', 'std_mole') %>%
        distinct() %>%
        join(gr,
             gr_raw_data$CITYGROUP == gr$CITYGROUP,
             gr_raw_data$std_mole == gr$std_mole, 'left')
    
    print(head(gr))
    print(head(arrange(gr,gr$GR1718)))
    print(head(arrange(gr,desc(gr$GR1718))))
    print(head(gr_with_id))
    # TODO: 输出gr_with_id; 获取raw_pha_id
    return(list(gr, gr_with_id))
}

modify_gr <- function(df,gr_cols){
    
    for(col in gr_cols){
        df <- df %>%
            mutate(tmp = ifelse(isNull(df[[col]]) |
                                    (df[[col]] > 10) |
                                    (df[[col]] < 0.1),
                                1,
                                df[[col]]))
        df <- df %>% withColumnRenamed('tmp', col)
    }
    return(df)
}


