cal_growth <- function(raw_data, id_city, max_month = 12){
    
    #TODO: 完整年用完整年增长，不完整年用不完整年增长
    if(max_month < 12){
        raw_data <- raw_data %>%
            filter(raw_data$Month <= max_month)
    }
    
    
    gr_raw_data <- join(raw_data, id_city,
                        raw_data$PHA == id_city$PHA, 'left') %>%
        drop_dup_cols()
    
    
    gr_raw_data <- mutate(gr_raw_data, 
                          City_Tier_2010=ifelse(
                              isNull(gr_raw_data$City_Tier_2010),
                              5,
                              gr_raw_data$City_Tier_2010
                          ))
    
    gr_raw_data <- rename(gr_raw_data, CITYGROUP = gr_raw_data$City_Tier_2010)
    
    
    gr <- gr_raw_data %>%
        group_by('Molecule', 'CITYGROUP', "Year") %>% 
        agg(value=sum(gr_raw_data$Sales))
    
    
    gr <- repartition(gr, 2L, 
                      gr$Molecule,
                      gr$CITYGROUP)
    
    con_schema <- structType(
        structField("Molecule", "string"),
        structField("CITYGROUP", "string"),
        structField("Year_2018", "double"),
        structField("Year_2019", "double")
    )
    
    gr <- 
        dapply(gr,
               function(x) {
                   library("tidyverse")
                   x <- unique(x)
                   x <- spread(x, "Year", "value", fill = 0)
                   return(x)
                   
               }, con_schema
        )
    gr <- gr %>%
        mutate(GR1819=gr$Year_2019/gr$Year_2018)
    
    
    gr <- modify_gr(gr, names(gr)[startsWith(names(gr),'GR')])
    
    gr_with_id <- gr_raw_data %>%
        select('PHA', 'ID', 'City', 'CITYGROUP', 'Molecule') %>%
        distinct() %>%
        join(gr,
             gr_raw_data$CITYGROUP == gr$CITYGROUP &
             gr_raw_data$Molecule == gr$Molecule, 'left') %>%
        drop_dup_cols()
    
    #print(head(gr, 100))
    #print(head(gr_with_id))
    # TODO: 输出gr_with_id; 获取raw_pha_id
    return(list(gr, gr_with_id))
}

modify_gr <- function(df,gr_cols){
    
    for(col in gr_cols){
        df[[col]] <- ifelse(isNull(df[[col]]) |
                                (df[[col]] > 10) |
                                (df[[col]] < 0.1),
                            1,
                            df[[col]])
    }
    return(df)
}
