

group_panel_by_seg <- function(panel_path, uni_ot, uni){
    panel <- read.df(panel_path,
                     'parquet')
    
    uni_panel_all <- select(uni %>% filter(uni$PANEL == 1), 
                            'PHA', 'BEDSIZE', 'PANEL', 'Seg')
    panel_all <- panel %>% join(uni_panel_all,
                                panel$HOSP_ID == uni_panel_all$PHA,
                                'inner') %>%
        drop_dup_cols()
    
    panel_all <- panel_all  %>%
        group_by('PHA', 'Province', 'City', 'Date', 'Molecule', 'Prod_Name',
                 'BEDSIZE', 'PANEL', 'Seg') %>%
        agg(Predict_Sales = sum(panel_all$Sales),
            Predict_Unit = sum(panel_all$Units))
    
    
    panel_drugincome<- uni_ot %>%
        filter(uni_ot$PANEL == 1)
    
    panel_drugincome <- panel_drugincome %>%
        group_by('Seg') %>%
        agg(DrugIncome_Panel = sum(panel_drugincome$Est_DrugIncome_RMB))
    
    
    panel <- panel %>% 
        join(uni_ot,
             panel$HOSP_ID == uni_ot$PHA, 'left') %>%
        drop_dup_cols()
    
    panel <- panel %>% 
        filter(panel$PANEL == 1)
    
    
    
    
    
    panel_seg <- panel %>% 
        group_by('Date','Prod_Name','Seg','Molecule') %>%
        agg(Sales_Panel=sum(panel$Sales),
            Units_Panel=sum(panel$Units))
    
    panel_seg <- panel_seg %>%
        join(panel_drugincome,
             panel_seg$Seg == panel_drugincome$Seg, 'left') %>%
        drop_dup_cols()
    

    return(list(panel_all, panel_seg))
}