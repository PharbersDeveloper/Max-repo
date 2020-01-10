

source('maxCal/PhOutlierReading.R', encoding = "UTF-8")
source('maxCal/PhPanelBySeg.R', encoding = "UTF-8")
source('maxCal/PhCombindUniverseAndFactor.R', encoding = "UTF-8")
source('maxCal/PhRemoveNegativeValue.R', encoding = "UTF-8")

mkt <- 'Sustenna'
time <- '201910'

uni_path <- paste0('/common/projects/max/Janssen/universe_', mkt)


uni_ot_path <- paste0('/common/projects/max/Janssen/universe_ot_', mkt)


panel_path <- paste0('/common/projects/max/Janssen/panel-result_',
                     mkt, '_', time)

factor_path <- paste0('/common/projects/max/Janssen/factor_', mkt)



uni_ot <- read_uni_ot(uni_ot_path)

uni <- read_universe(uni_path)

panel_results <- group_panel_by_seg(panel_path, uni_ot,
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

write.df(max, paste0("/common/projects/max/Janssen/Janssen_MAX_result_",
                     time, mkt, "_hosp_level"), 
         "parquet", "overwrite")



