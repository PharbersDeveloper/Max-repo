
#原始数据
raw_data_path <- 
    "hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/简单整合的az201701-201909_sanofi201801-201911原始数据_去重版_20200210"

#universe
#uni_path <- "hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/universe_az_sanofi_2spec"
uni_path <- "hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/universe_az_sanofi_base"

#原始医院与pha匹配
cpa_pha_mapping_path <- "hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/医院匹配_20191031"

#分子匹配模型
mkt_path = "hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/az_sanofi清洗_ma"
##最小产品单位标准化
map_path = "hdfs://192.168.100.137:8020//common/projects/max/AZ_Sanofi/az_sanofi清洗_ma"

#月份文件夹
c_month = "1701-1911"

#不足12个月的年份
year_missing = c(2019)

#缺失年份的月份数
max_month = 11

#待清洗文件输出路径
need_cleaning_path = 
    paste0('Y:/MAX/AZ/UPDATE/','1910','/待清洗',Sys.Date(),'.xlsx')

#连续性文件输出路径
continuity_path = 
    paste0('Y:/MAX/AZ/UPDATE/','1910','/连续性',Sys.Date(),'.xlsx')


new_hospital_path <- paste0('Y:/MAX/AZ/UPDATE/','1910/2019new_hospital','.xlsx')

#增长率路径
gr_path = paste0('Y:/MAX/AZ/UPDATE/','1910','/增长率',Sys.Date(),'.xlsx')
gr_path_online = "/common/projects/max/AZ_Sanofi/gr"

#第一阶段补数结果路径
raw_data_adding_path <- "/common/projects/max/AZ_Sanofi/raw_data_adding"

#第二阶段补数结果路径
adding_data_new_path <- "/common/projects/max/AZ_Sanofi/adding_data_new"


#panel路径
panel_path <- paste0("/common/projects/max/AZ_Sanofi/panel-result_AZ_Sanofi_201701-201911_20200212")
# panel_path_local <- paste0('Y:/MAX/AZ/UPDATE/','1912',
#                            '/panel_hosp+ym_level_201701-201912_',Sys.Date(),'.csv')

std_names <- c('PHA','ID','Year','Month','Molecule','Brand','Form',
               'Specifications','Pack_Number','Manufacturer','Sales','Units',
               'Province', 'City', 'Corp', 'Route')

#时间范围路径
original_range_path <- paste0("/common/projects/max/AZ_Sanofi/original_range")


#min1构成
min_content <- c('Brand',
                 "Form",
                 'Specifications',
                 "Pack_Number",
                 'Manufacturer')


need_cleaning_cols <- c("Molecule", min_content, "min1", 'Route', 'Corp')
