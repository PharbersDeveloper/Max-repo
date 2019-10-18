# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #
# Purpose:      factorize and validate
# programmer:   Jingxian Lu
# Date:         Oct-17-2016
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ #

require(openxlsx)
require(dplyr)
require(tidyr)
#require(sca)
require(readr)
require(stringr)
require(stringi)
# require(rPython)
require(data.table)
# variables for test



MKT <- list(
  c('凯纷', "凯纷")
)
# City15 <- c('上海市',
#             '北京市',
#             '广州市',
#             '天津市',
#             '成都市',
#             '杭州市',
#             '南京市',
#             '福厦泉市',
#             '武汉市',
#             '青岛市',
#             '哈尔滨市',
#             '苏州市',
#             '西安市',
#             '济南市',
#             '宁波市')
# ("D:/Pharbers文件/Eisai/MAX/6MKT")

universe <- read.xlsx("E:/MAX/Tide/MODEL/凯纷/000ref/universe.xlsx")
pha_city <- universe[,c("City",'Panel_ID')]
bed <- universe$Panel_ID[universe$BEDSIZE >99]
p <- universe$Panel_ID[universe$PANEL %in% c(1,'1')]
# bridge <-
#   read.xlsx("./02_Inputs/City VS Tier.xlsx")
# City37 <-
#   read.xlsx("D:/Pharbers文件/Eisai/MAX/6MKT/02_Inputs/37Cities.xlsx")
# y <- "6"
# y1 <- "611"

# 更改panel路径
factorize <- function(mkt, y1) {
  # mkt <- '凯纷'
  # y1 <- '904'
  ###  factorize
  # fct <- read.xlsx(paste("D:/Pharbers文件/Eisai/MAX/6MKT/03_Outputs/max_outputs/", mkt, "_factor&city.xlsx", sep = ""))
  
  #第一次跑用这个
  # fct <-
  #   read.csv(
  #     paste('E:/MAX/Tide/MODEL/',mkt,'/050factor/', mkt,
  #           "_factor&city_full_1.csv", sep = ""),
  #     header = TRUE,
  #     stringsAsFactors = FALSE
  #   )

  #优化factor后用这个
  fct <-
    read.csv(
      paste('E:/MAX/Tide/MODEL/',mkt,'/050factor/', mkt,
            "_factor&city_full.csv", sep = ""),
      header = TRUE,
      stringsAsFactors = FALSE
    )[,c(1,4)]
  names(fct)[names(fct)=='factor_new'] <- 'factor'

   
  # read panel-hospitals' Panel_ID
  
  # max_detail <- read.csv(paste("D:/Pharbers文件/Eisai/MAX/6MKT/02_Inputs/20000/MAX_", mkt, "_20000_wo_ot_15.csv", sep = ""),
  #                        header = TRUE,
  #                        stringsAsFactors = FALSE,
  #                        fileEncoding = "UTF-8")
  # max_detail1 <- read.csv(paste("D:/Pharbers文件/Eisai/MAX/6MKT/02_Inputs/20000/MAX_", mkt, "_20000_wo_ot_15h1.csv", sep = ""),
  #                         header = TRUE,
  #                         stringsAsFactors = FALSE
  # )
  # max_detail2 <- read.csv(paste("D:/Pharbers文件/Eisai/MAX/6MKT/02_Inputs/20000/MAX_", mkt, "_20000_wo_ot_15h2.csv", sep = ""),
  #                         header = TRUE,
  #                         stringsAsFactors = FALSE
  # )
  
  # read max result
  max_detail1 <-
    fread(
      paste(
        'L:/MAX result/Tide/1906/MAX_',
        mkt,
        '_20000_wo_ot_',
        y1,
        '.csv',
        sep = ""
      ),
      stringsAsFactors = F,
      header = T
    )
  # max_detail2 <- read.csv(paste("D:/Pharbers文件/Eisai/MAX/6MKT/02_Inputs/20000/MAX_", mkt, "_20000_wo_ot_15h2.csv", sep = ""),
  #                         header = TRUE,
  #                         stringsAsFactors = FALSE
  # )
  names(max_detail1)[names(max_detail1) == '(空)'] <- 'Predict_Sales'
  names(max_detail1)[names(max_detail1) == '(2)'] <- 'Predict_Unit'
  names(max_detail1)[names(max_detail1) == 'Predict_Unit (3)'] <- 'Predict_Sales'
  names(max_detail1)[names(max_detail1) == 'Sum(Predict_Sales)'] <- 'Predict_Sales'
  names(max_detail1)[names(max_detail1) == 'Sum(Predict_Unit)'] <- 'Predict_Unit'
  names(max_detail1)[names(max_detail1) == 'sum(Predict_Sales)'] <- 'Predict_Sales'
  names(max_detail1)[names(max_detail1) == 'sum(Predict_Unit)'] <- 'Predict_Unit'
  names(max_detail1)[names(max_detail1) == 'sum(Predict_Units)'] <- 'Predict_Unit'
  max_detail1$Predict_Sales <- as.numeric(max_detail1$Predict_Sales)
  max_detail1$Predict_Unit <- as.numeric(max_detail1$Predict_Unit)
  print(sum(max_detail1$Predict_Sales, na.rm = T))
  max_detail <- max_detail1
  max_detail$Predict_Unit[is.na(max_detail$Predict_Unit)]=0
  max_detail$Predict_Sales[is.na(max_detail$Predict_Sales)]=0
  if(dim(max_detail[(!(max_detail$Panel_ID %in% p)) &
                    (max_detail$Predict_Sales < 0), ])[1] > 0) {
    max_detail[(!(max_detail$Panel_ID %in% p)) &
                 (max_detail$Predict_Sales < 0), ]$Predict_Unit <-
      0
    max_detail[(!(max_detail$Panel_ID %in% p)) &
                 (max_detail$Predict_Sales < 0), ]$Predict_Sales <-
      0
  }
  if (dim(max_detail[(!(max_detail$Panel_ID %in% p)) &
                     (max_detail$Predict_Unit < 0), ])[1] > 0) {
    max_detail[(!(max_detail$Panel_ID %in% p)) &
                 (max_detail$Predict_Unit < 0), ]$Predict_Sales <-
      0
    max_detail[(!(max_detail$Panel_ID %in% p)) &
                 (max_detail$Predict_Unit < 0), ]$Predict_Unit <-
      0
  }
  print(sum(max_detail1$Predict_Sales))
  print(sum(max_detail$Predict_Sales))
  # multiply factors
  f_sa_un <- left_join(max_detail, fct, by = "City")
  f_sa_un$factor[f_sa_un$Panel_ID %in% p] <- 1
  
  f_sa_un$f_sales <-
    as.numeric(f_sa_un$`Predict_Sales`) * as.numeric(f_sa_un$factor)
  f_sa_un$f_units <-
    as.numeric(f_sa_un$`Predict_Unit`) * as.numeric(f_sa_un$factor)
  # write.csv(f_sa_un, paste("D:/Pharbers文件/Eisai/MAX/6MKT/03_Outputs/20000/MAX_", mkt, "_factorized.csv", sep = ""),
  #           row.names = FALSE)
  
  
  ### add ot
  
  
  
  # no_ot <- read.csv(paste("D:/Pharbers文件/Eisai/MAX/6MKT/03_Outputs/20000/MAX_", mkt_name, "_factorized.csv", sep = ""),
  #                   header = TRUE,
  #                   stringsAsFactors = FALSE)
  
  
  
  # read outliers and replace ot-hospitals
  # outlier <-
  #   read.csv(paste("F:/LJX/Astellas/02_Inputs/", mkt, 
  #                   "_outlier.csv", sep = ""), header = T, stringsAsFactors = F)
  # outlier <- outlier$x
  
  panel15 <-
    read.xlsx(paste0('E:/MAX/Tide/MODEL/凯纷/010Panel_data/凯纷_Panel_20',y1,'.xlsx'))

  # panel15 <-
  #   read.xlsx(paste0(
  #     'G:/Pharbers/Astellas/YTD/1',
  #     substr(y1,1,1),
  #     '/',
  #     mkt,'市场',
  #     '_panel1',
  #     y1,
  #     '.xlsx'
  #   ))
  # 
  
  # panel15 <-
  #   read.xlsx(paste0(
  #     'G:/Pharbers/Astellas/YTD/141516180104/',
  #     mkt,'市场',
  #     '_panel',
  #     '.xlsx'
  #    ))
  # 
  panel15 <- panel15 %>%
    filter(HOSP_ID %in% p)
  
  
  colnames(panel15)[which(colnames(panel15) == "HOSP_ID")] <-
    "Panel_ID"
  colnames(panel15)[which(colnames(panel15) == "Prod_Name")] <-
    "Product"
  ot <- panel15 %>%
    filter(Panel_ID %in% p) %>%
    left_join(pha_city, by = "Panel_ID") %>%
    group_by(Panel_ID, Date, City, Product) %>%
    summarise(Sales = sum(Sales, na.rm = TRUE),
              Units = sum(Units, na.rm = TRUE))
  ot$factor <- 1
  ot$f_sales <- ot$factor * ot$Sales
  ot$f_units <- ot$factor * ot$Units
  
  no_ot <- f_sa_un
  colnames(no_ot)[which(colnames(no_ot) == "Predict_Sales")] <-
    "Sales"
  colnames(no_ot)[which(colnames(no_ot) == "Predict_Unit")] <-
    "Units"
  colnames(no_ot)[which(colnames(no_ot) == "Prod_Name")] <-
    "Product"
  no_ot1 <- no_ot %>%
    filter(!Panel_ID %in% p)
  print(unique(no_ot1$City[no_ot1$factor == ""]))
  
  chk <- rbind(data.frame(ot), data.frame(no_ot1))
  print(sum(chk$f_sales))
  sum(chk$Sales)

  
  #第一次跑用这个
  # write.csv(
  #   chk,
  #   paste(
  #     "E:/MAX/Tide/MODEL/",mkt,"/060factorized/",
  #     mkt,
  #     "_Factorized_Units&Sales_WITH_OT18", ".csv",
  #     sep = ""
  #   ), row.names = F
  # )
  
  # write final maxed data
  write.csv(
    chk,
    paste(
      "L:/MAX result/Tide/1906/",
      mkt,
      "_Factorized_Units&Sales_WITH_OT",y1, ".csv",
      sep = ""
    ), row.names = F
  )

  
  
  
  
  # write.table(chk, paste("D:/Pharbers文件/Eisai/MAX/6MKT/03_Outputs/20000/", mkt, "_Factorized_Units&Sales_WITH_OT_ljx.txt", sep = ""),
  #           row.names = FALSE,fileEncoding = "GBK",sep = ";")
  
}

for (i in c(1:length(MKT))) {
  for (y in c('17','18','19')) {
    print(MKT[[i]][1])
    factorize(MKT[[i]][1], y)
  }
}
# factorize(MKT[[i]][2], MKT[[i]][1], "5", "5")

