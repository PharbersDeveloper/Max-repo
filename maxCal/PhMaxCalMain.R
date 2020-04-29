

source('maxCal/PhOutlierReading.R', encoding = "UTF-8")
source('maxCal/PhPanelBySeg.R', encoding = "UTF-8")
source('maxCal/PhCombindUniverseAndFactor.R', encoding = "UTF-8")
source('maxCal/PhRemoveNegativeValue.R', encoding = "UTF-8")

if(F){
    mkt <- 'AZ16'
    time_l <- 201901
    time_r <- 201911
    time <- paste(time_l, time_r, sep = '-')
    
    
    if(mkt %in% c('SNY6','SNY10','SNY12','SNY13','AZ12','AZ18','AZ21')){
        uni_path <- paste0('/common/projects/max/AZ_Sanofi/universe_az_sanofi_onc')
    }else if(mkt %in% c('SNY5', 'SNY9', 'AZ10' ,'AZ11', 'AZ15', 'AZ16', 'AZ14', 'AZ26', 'AZ24')){
        uni_path <- paste0('/common/projects/max/AZ_Sanofi/universe_az_sanofi_mch')
    }else{
        uni_path <- paste0('/common/projects/max/AZ_Sanofi/universe_az_sanofi_base')
    }
}


max_cal <- function(mkt, if_base = F, if_box = F){
    print(mkt)
    uni_path <- choose_uni(mkt)
    
    time_l <- choose_months(mkt)[[1]]
    time_r <- choose_months(mkt)[[2]]
    time <- paste(time_l, time_r, sep = '-')
    print(time)
    uni_ot_path <- paste0(project_path,"universe/universe_ot_", mkt)
    
    
    # panel_path <- paste0('/common/projects/max/AZ_Sanofi/',
    #                      'panel-result_AZ_Sanofi_201701-201911_20200212')
    # panel_path <- paste0("/common/projects/max/AZ_Sanofi/panel-result_AZ_Sanofi_201701-202001")
    
    factor_path <- paste0(project_path,"factor/factor_", mkt)
    
    if(if_base){
        factor_path <- base_factor_path
    }
    
    
    
    uni_ot <- read_uni_ot(uni_ot_path)
    
    uni <- read_universe(uni_path)
    
    if(if_box){
        ori_panel <- read.df(panel_box_path,
                             'parquet')
    }else{
        ori_panel <- read.df(panel_path,
                             'parquet')
    }
    
    ori_panel <- filter(ori_panel, (ori_panel$DOI %in% mkt) &
                            (ori_panel$Date >= time_l) &
                            (ori_panel$Date <= time_r))
    
    panel_results <- group_panel_by_seg(ori_panel, uni_ot,
                                        uni)
    
    
    # 第一个整理成max的格式，包含了所有在universe的panel列标记为1的医院，
    # 当作所有样本医院的max
    panel <- panel_results[[1]]
    # 第二个整理成seg层面，包含了所有在universe_ot的panel列标记为1的医院，
    # 可以用来得到非样本医院的max
    panel_seg <- panel_results[[2]]
    
    
    # 将非样本的segment和factor等信息合并起来
    universe_wo_panel <- get_uni_with_factor(factor_path, uni)
    
    
    
    # 为这些非样本医院匹配上样本金额、产品、年月、所在segment的drugincome之和
    max <- universe_wo_panel %>% 
        join(panel_seg, universe_wo_panel$Seg == panel_seg$Seg, 'left') %>%
        drop_dup_cols()
    
    
    
    
    # 预测值等于样本金额乘上当前医院drugincome再除以所在segment的drugincome之和
    max <- max %>%
        mutate(Predict_Sales = (max$Sales_Panel/max$DrugIncome_Panel)*
                   max$Est_DrugIncome_RMB,
               Predict_Unit = (max$Units_Panel/max$DrugIncome_Panel)*
                   max$Est_DrugIncome_RMB)
    
    # 为什么有空，因为部分segment无样本或者样本金额为0
    max <- filter(max, !isNull(max$Predict_Sales))
    
    max <- remove_nega(max, c('Predict_Sales', 'Predict_Unit'))
    
    
    # 乘上factor
    max <- max %>%
        mutate(Predict_Sales = max$Predict_Sales * max$factor,
               Predict_Unit = max$Predict_Unit * max$factor) %>%
        select('PHA', 'Province', 'City','Date', 'Molecule','Prod_Name',
               'BEDSIZE', 'PANEL', 'Seg',
               'Predict_Sales', 'Predict_Unit')
    
    
    # 合并样本部分
    max <- rbind(max, panel)
    if(!if_base){
        max <- repartition(max, 2L)
        if(if_box){
            write.df(max, paste0(project_path,"MAX_result/MAX_result_",
                                 time, mkt, "_hosp_level_box"), 
                     "parquet", "overwrite")
        }else{
            write.df(max, paste0(project_path,"MAX_result/MAX_result_",
                                 time, mkt, "_hosp_level"), 
                     "parquet", "overwrite")
        }
        
    }
    
    if(T){
        # max <- read.df(paste0("/common/projects/max/AZ_Sanofi/MAX_result/AZ_Sanofi_MAX_result_",
        #                       time, mkt, "_hosp_level"), "parquet")
        max_c <- max %>% filter(max$BEDSIZE > 99)
        max_c <- group_by(max_c, 'Province', 'City', 'PANEL',"Prod_Name", 'Date') %>%
            agg(Predict_Sales = sum(max_c$Predict_Sales),
                Predict_Unit = sum(max_c$Predict_Unit))
        
        max_c <- collect(max_c)
        print(sort(unique(max_c$Date)))
        if(if_box){
            openxlsx::write.xlsx(max_c, 
                                 paste0(project_path_local,'MODEL/',mkt,'/040max_output/',mkt,
                                        '_MAX_result_100bed_',time,'_box', Sys.Date(), '.xlsx'))
        }else{
            openxlsx::write.xlsx(max_c, 
                                 paste0(project_path_local,'MODEL/',mkt,'/040max_output/',mkt,
                                        '_MAX_result_100bed_',time, Sys.Date(), '.xlsx'))
        }
        
    }
}


for(i in all_models){
    max_cal(i, if_box = F)
}

for(i in other_models){
    max_cal(i, if_box = T)
}



