

source('maxCal/PhOutlierReading.R', encoding = "UTF-8")
source('maxCal/PhPanelBySeg.R', encoding = "UTF-8")
source('maxCal/PhCombindUniverseAndFactor.R', encoding = "UTF-8")
source('maxCal/PhRemoveNegativeValue.R', encoding = "UTF-8")

mkt <- 'SNY1'
time_l <- 201701
time_r <- 201911
time <- paste(time_l, time_r, sep = '-')


if(mkt %in% c('SNY6','SNY10','SNY12','SNY13')){
    uni_path <- paste0('/common/projects/max/AZ_Sanofi/universe_az_sanofi_onc')
}else if(mkt %in% c('SNY5', 'SNY9', 'AZ11')){
    uni_path <- paste0('/common/projects/max/AZ_Sanofi/universe_az_sanofi_mch')
}else{
    uni_path <- paste0('/common/projects/max/AZ_Sanofi/universe_az_sanofi_base')
}



uni_ot_path <- paste0('/common/projects/max/AZ_Sanofi/universe/universe_ot_', mkt)


panel_path <- paste0('/common/projects/max/AZ_Sanofi/',
                     'panel-result_AZ_Sanofi_201701-201911_20200212')

factor_path <- paste0('/common/projects/max/AZ_Sanofi/factor/factor_', mkt)

if(F){
    factor_path <- paste0('/common/projects/max/AZ_Sanofi/factor/factor_', 'base')
}



uni_ot <- read_uni_ot(uni_ot_path)

uni <- read_universe(uni_path)

ori_panel <- read.df(panel_path,
                 'parquet')
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

write.df(max, paste0("/common/projects/max/AZ_Sanofi/MAX_result/AZ_Sanofi_MAX_result_",
                     time, mkt, "_hosp_level"), 
         "parquet", "overwrite")
if(T){
    # max <- read.df(paste0("/common/projects/max/AZ_Sanofi/MAX_result/AZ_Sanofi_MAX_result_",
    #                       time, mkt, "_hosp_level"), "parquet")
    max_c <- max %>% filter(max$BEDSIZE > 99)
    max_c <- group_by(max_c, 'Province', 'City', 'PANEL',"Prod_Name", 'Date') %>%
        agg(Predict_Sales = sum(max_c$Predict_Sales),
            Predict_Unit = sum(max_c$Predict_Unit))
    
    max_c <- collect(max_c)
    openxlsx::write.xlsx(max_c, 
                         paste0('y:/MAX/AZ/MODEL/',mkt,'/040max_output/',mkt,
                                '_MAX_result_100bed_',time,'.xlsx'))
}



