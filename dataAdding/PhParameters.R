

project_path <- "/common/projects/max/Sankyo/"
project_path_local <- "Y:/MAX/Sankyo/"


#原始数据
raw_data_path <- 
    paste0(project_path,
           "raw_data")

others_box_path <- 
    paste0(project_path,"others_box")

#"/common/projects/max/AZ_Sanofi/raw_data/简单整合的az201701-201909_sanofi201801-202001"
#universe
#uni_path <- "/common/projects/max/AZ_Sanofi/universe_az_sanofi_2spec"
uni_base_path <- paste0(project_path,"universe_base")

#原始医院与pha匹配
cpa_pha_mapping_path <- paste0(project_path,"cpa_pha_mapping")

#分子匹配模型
mkt_path = paste0(project_path,"mkt_mapping")
##最小产品单位标准化
map_path = paste0(project_path,"prod_mapping")

#月份文件夹
#c_month = "2001"

#不足12个月的年份
year_missing = c()

#缺失年份的月份数
max_month = 12

#待清洗文件输出路径
# need_cleaning_path = 
#     paste0(project_path_local,'UPDATE/','1910','/待清洗',Sys.Date(),'.xlsx')

#连续性文件输出路径
# continuity_path = 
#     paste0(project_path_local,'UPDATE/','1910','/连续性',Sys.Date(),'.xlsx')


new_hospital_path <- paste0(project_path_local,'UPDATE/',
                            '2001/2019new_hospital','.xlsx')

#增长率路径
gr_path = paste0(project_path_local,'UPDATE/','2001','/增长率',Sys.Date(),'.xlsx')
gr_path_online = paste0(project_path,"gr")

#第一阶段补数结果路径
raw_data_adding_path <- paste0(project_path,"raw_data_adding")

#第二阶段补数结果路径
adding_data_new_path <- paste0(project_path,"adding_data_new")


#panel路径
panel_path <- 
    paste0(project_path,"panel-result")
panel_box_path <- 
    paste0(project_path,"panel_box-result")
# panel_path_local <- paste0('Y:/MAX/AZ/UPDATE/','1912',
#                            '/panel_hosp+ym_level_201701-201912_',Sys.Date(),'.csv')

std_names <- c('PHA','ID','Year','Month','Molecule','Brand','Form',
               'Specifications','Pack_Number','Manufacturer','Sales','Units',
               'Province', 'City', 'Corp', 'Route')

#时间范围路径
original_range_path <- paste0(project_path,"original_range")


#min1构成
min_content <- c('Brand',
                 "Form",
                 'Specifications',
                 "Pack_Number",
                 'Manufacturer')


need_cleaning_cols <- c("Molecule", min_content, "min1", 'Route', 'Corp')



published_l_path <- "y:/MAX/Sanofi/UPDATE/2001/Published2019.xlsx"
published_r_path <- "y:/MAX/Sanofi/UPDATE/2001/Published2020.xlsx"


price_path <- paste0(project_path,"price")
price_box_path <- paste0(project_path,"price_box")

adding_data_path <- paste0(project_path,"adding_data_tmp")


raw_data_tmp_path <- paste0(project_path,"raw_data_tmp")


model_month_l <- 201901
model_month_r <- 201912


poi_path <- paste0(project_path_local,'UPDATE/',"2001/poi.xlsx")




unpublished_path <- "y:/MAX/Sanofi/UPDATE/2001/Unpublished2020.xlsx"




choose_uni <- function(mkt){
    if(mkt %in% c('SNY6','SNY10','SNY12','SNY13','AZ12','AZ18','AZ21')){
        uni_path <- paste0(project_path,"universe_az_sanofi_onc")
    }else if(mkt %in% c('SNY5', 'SNY9', 'AZ10' ,'AZ11', 'AZ15', 'AZ16', 'AZ14', 'AZ26', 'AZ24')){
        uni_path <- paste0(project_path,"universe_az_sanofi_mch")
    }else{
        uni_path <- paste0(project_path,"universe_base")
    }
    return(uni_path)
}

choose_months <- function(mkt){
    time_l <- 201801
    time_r <- 202002
    if(mkt %in% c('SNY4', 'SNY5', 'SNY7', 'SNY9', 'SNY11', 'SNY14')){
        time_l <- 201801
    }
    if(mkt %in% rest_models){
        time_r <- 202001
    }
    if(F){
        time_l <- 202002
        time_r <- 202002
    }
    return(list(time_l, time_r))
}

all_models <- c('CV IV', 'LIX', 'LOX Tab', 'MVT', 'OLM')

rest_models <- c()

base_factor_path <- paste0(project_path,"factor/factor_", 'base')

min1_sep <- ''

