

trans_raw_data_for_adding <- function(raw_data, id_city, gr_with_id){
    gr_with_id <- gr_with_id[,c('CITYGROUP','std_mole',
                                names(gr_with_id)[startsWith(names(gr_with_id),
                                                             'GR1')])] %>%
        distinct()
    seed <- filter(raw_data, !isNull(raw_data$PHA))
    seed <- arrange(seed, desc(seed$Year))
    seed <- join(seed, id_city, seed$PHA==id_city$PHA, 'left')
    names(seed)[names(seed)=='City_Tier_2010']<-'CITYGROUP'
    seed <- seed %>%
        join(gr_with_id,
             seed$std_mole==gr_with_id$std_mole &
                 seed$CITYGROUP==gr_with_id$CITYGROUP, 'left') %>%
        drop_dup_cols()
    # seed <- seed %>%
    #     mutate(GR1718=ifelse(isNull(seed$GR1718),1,seed$GR1718))
    # seed <- seed %>%
    #     mutate(GR1819=seed$GR1718,
    #            PHA=seed$PHA)
    
    
    return(seed)
    
}