require(openxlsx)
require(dplyr)
require(tidyr)
require(sca)
require(readr)
require(stringr)
require(stringi)
require(data.table)
require(RODBC)
require(sqldf)
require(tidyverse)

mkt.defi<-read.xlsx('E:/MAX/Tide/Global/通用名企业层面集中度_pb.xlsx')

MKT <- list(c('凯时', "凯时"),
            
            c('凯那', "凯那"))
koufu_pi <- 
  read.xlsx('E:/MAX/Tide/Global/泰德产品匹配表.xlsx')
#koufu_pi$Market<-'奥鸿'
ims_city_data <-  fread(paste0('L:/全原盘数据/ssd2备份/MAX/Pfizer/',
                               'Making ims flat file/201906/cn_IMS_Sales_Fdata_201906_1.txt'),
                        header = T, stringsAsFactors = F)

ims_city_data$Pack_ID<-stri_sub(paste0('000000000',ims_city_data$Pack_ID),-7,-1)
unique(nchar(ims_city_data$Pack_ID))

ims_city_data_v2 <- ims_city_data
colnames(ims_city_data_v2)[colnames(ims_city_data_v2) == "Period_Code"] <- "Date"
ims_city_data_v2$Date <- gsub('M', '', ims_city_data_v2$Date)


# ims_city_data_v2$Date <- gsub("M0[123]", "Q1", ims_city_data_v2$Date)
# ims_city_data_v2$Date <- gsub("M0[456]", "Q2", ims_city_data_v2$Date)
# ims_city_data_v2$Date <- gsub("M0[789]", "Q3", ims_city_data_v2$Date)
# ims_city_data_v2$Date <- gsub("M1[012]", "Q4", ims_city_data_v2$Date)
ims_city_data1 <- ims_city_data_v2 %>%
  group_by(Geography_id, Pack_ID, Date) %>%
  summarise(Sales = sum(LC, na.rm = T))

unique(ims_city_data1$Geography_id)
print(sum(ims_city_data1$Sales, na.rm = T))
# 6.062452e+12 6.228165e+12
# year <- "6Q3"

info <- ims_city_data1 %>%
  filter(!(Geography_id %in% c('32C', "CHACACACACACAAA"))) %>%
  group_by(Date, Geography_id, Pack_ID) %>%
  summarise(Sales = sum(Sales, na.rm = T))
# info$Pack_ID <- str_sub(paste("0000000", info$Pack_ID, sep = ""), -7, -1)

names(koufu_pi)
names(koufu_pi)[names(koufu_pi) == '标准商品名'] <- '商品名_标准'
names(koufu_pi)[names(koufu_pi) == 'Pack_ID'] <- 'Pack_ID'
names(koufu_pi)[names(koufu_pi) == '标准剂型'] <- '剂型_标准'
#names(koufu_pi)[names(koufu_pi) == 'Market'] <- '市场'
names(koufu_pi)[names(koufu_pi) == '标准通用名'] <- 'S_Molecule.Name'


panel_all3 <- unique(koufu_pi[c( '商品名_标准', 'Pack_ID', '剂型_标准', 
                                'S_Molecule.Name')]) %>% 
  filter(!is.na(Pack_ID))%>%left_join(mkt.defi[,c('市场','药品名称')]%>%unique(),
                                      by=c('S_Molecule.Name'='药品名称'))


# unique(panel_all3$市场)
# panel_all3$市场 <- 'O'
panel_all4 <- panel_all3[c('市场', '商品名_标准', 'Pack_ID', 'S_Molecule.Name')] %>% unique()
names(panel_all4) <- c('DOI', 'prod_s', 'packid', 'Molecule')
unique(panel_all4$DOI)
# panel_all4 <- read.xlsx("./02_Inputs/prod_mkt_packid.xlsx")
# info$Pack_ID <- iconv(info$Pack_ID, to = "UTF-8")
# panel_all4$packid <- iconv(panel_all4$packid, to = "UTF-8")
panel_all4_v1 <- group_by(panel_all4, packid, DOI, Molecule) %>% 
  mutate(n = nchar(prod_s)) %>% arrange(n) %>%
  summarise(prod_s = first(prod_s))
panel_all4_v1$packid<-stri_pad_left(panel_all4_v1$packid,7,0)


unique(panel_all4_v1$DOI)
class(panel_all4_v1$packid)
info$Pack_ID<-as.character(info$Pack_ID)
info1 <- left_join(info, panel_all4_v1, 
                   by = c("Pack_ID" = "packid"))
unique(info1$DOI)
info1_v1 <- info1 %>%
  filter(!is.na(DOI)) %>%
  group_by(Pack_ID,DOI, Date, Geography_id , prod_s, Molecule) %>%
  summarise(ims_poi_vol = sum(Sales, na.rm = T))
# info1_v1$ims_poi_vol_w <- ifelse(info1_v1$`Molecule` %in% c07, info1_v1$ims_poi_vol * 0.25,
#                      ifelse(info1_v1$`Molecule` %in% c08, info1_v1$ims_poi_vol * 0.2,
#                             info1_v1$ims_poi_vol))

city_cname <- read.xlsx("L:/全原盘数据/F盘备份/LJX/Eisai/02_Inputs/ims_cname.xlsx")
info2 <- left_join(info1_v1, city_cname, by = c("Geography_id" = "GEO_CD"))
unique(info2$Geography_id[is.na(info2$City)])
info2$City[is.na(info2$City)] <- 'CHPA'
info3 <- info2# %>% filter(City %in% City39)
print(sum(info2$ims_poi_vol[info2$City %in% 'CHPA' & 
                              substr(info2$Date, 4, 4) %in% '7']))

y = '7'
{
  # info3_v1 <- filter(info3, as.numeric(Date) > 201509 &
  #                      as.numeric(Date) < 201610)
  info3_v1 <- filter(info3, substr(Date, 4, 4) %in% y)
  #info3_v1 <- filter(info3, substr(Date, 4, 4) %in% c('6','7','8') & DOI%in%c('11','8'))
  
  print(unique(info3_v1$Date))
  ims_info <- group_by(info3_v1, DOI, City, prod_s) %>%
    summarise(ims_poi_vol = sum(ims_poi_vol))
  ims_info1 <- group_by(ims_info, DOI, City) %>%
    summarise(ims_mkt_vol = sum(ims_poi_vol))
  ims_info2 <- left_join(ims_info, ims_info1, by = c('DOI', 'City'))
  ims_info2$ims_share <- ims_info2$ims_poi_vol / ims_info2$ims_mkt_vol
  for(mkt in 1:length(MKT)) {
    print(MKT[[mkt]][1])
    # ims_info3 <- ims_info2[ims_info2$DOI %in% MKT[[mkt]][1] & 
    #                          ims_info2$prod_s %in% 
    #                          c('他莫昔芬', '特茉芬', '枢瑞', '法乐通'), 
    #                        c('City', 'prod_s', 'ims_poi_vol', 'ims_share')]
    ims_info3 <- ims_info2[ims_info2$DOI %in% MKT[[mkt]][1], 
                           c('City', 'prod_s', 'ims_poi_vol', 'ims_share')]
    names(ims_info3) <- c('city', 'poi', 'ims_poi_vol', 'ims_share')
    write.xlsx(
      ims_info3,
      paste(
        "J:/MAX/Qilu/MODEL/",MKT[[mkt]][1],"/011IMS_data/",
        MKT[[mkt]][1],
        "_ims_info1", y, ".xlsx",
        sep = ""
      )
    )
  }
}


# IMS by month
{
  ims_bymonth <- group_by(info3[info3$City %in% 'CHPA',], DOI, Date, prod_s) %>%
    summarise(ims_poi_vol = sum(ims_poi_vol, na.rm = T)) %>% ungroup()
  
  ims_bymonth$prod_s[is.na(ims_bymonth$prod_s)] <- 'empty'
  # wb <- createWorkbook()
  for (i in 1:length(MKT)) {
    print(MKT[[i]][1])
    # ims_bymonth2 <-
    #   filter(ims_bymonth, DOI %in% MKT[[i]][1] & 
    #            prod_s %in% 
    #            c('他莫昔芬', '特茉芬', '枢瑞', '法乐通'))
    ims_bymonth2 <-
      filter(ims_bymonth, DOI %in% MKT[[i]][1])
    ims_bymonth3 <- group_by(ims_bymonth2, Date) %>%
      summarise(IMS = sum(ims_poi_vol))
    ims_bymonth4 <- ims_bymonth2 %>%
      spread(prod_s, ims_poi_vol, fill = 0) %>%
      left_join(ims_bymonth3, by = "Date")
    for (j in unique(ims_bymonth2$prod_s)) {
      ims_bymonth4[paste(j, "Share", sep = "")] <- ims_bymonth4[j] /
        ims_bymonth4$IMS
    }
    # addWorksheet(wb, MKT[[i]][2])
    # writeData(wb, MKT[[i]][2], ims_bymonth4)
    print(sum(ims_bymonth4$IMS)==sum(ims_bymonth2$ims_poi_vol))
    write.xlsx(ims_bymonth4, paste(
      "J:/MAX/Qilu/MODEL/",MKT[[i]][1],"/011IMS_data/IMS_by_month_",
      MKT[[i]][1],
      "_weight.xlsx",
      sep = ""
    ))
  }
  # saveWorkbook(wb, paste0('D:/Pharbers文件/Eisai/MAX/IMSBYMONTH/IMS_by_month_', 
  #                         MKT[[i]][2], '.xlsx'),
  #              overwrite = T)
}

# IMS 趋势线
{
  mkt <- '6'
  info4 <- info3 %>%
    group_by(DOI, City, Date, prod_s) %>%
    summarise(IMS_Sales = sum(ims_poi_vol))
  info5 <- info3 %>%
    group_by(DOI, City, Date) %>%
    summarise(IMS_Sales = sum(ims_poi_vol))
  info6 <- left_join(info4, info5, by = (c("DOI", "City", "Date")))
  print(dim(info4)[1] == dim(info6)[1])
  info6$Share <- info6$IMS_Sales.x / info6$IMS_Sales.y
  info6$Source <- "IMS Share"
  names(info6)[names(info6) == 'IMS_Sales.x'] <- 'Prod'
  names(info6)[names(info6) == 'IMS_Sales.y'] <- 'Mkt'
  # mktname <-
  #   data.frame(unlist(MKT)[seq(1, length(MKT) *2, 2)], unlist(MKT)[seq(2, length(MKT) *2, 2)],
  #              stringsAsFactors = F)
  # colnames(mktname) <- c("DOI", "Market")
  # mktname$DOI <- iconv(mktname$DOI, to = "UTF-8")
  # info7 <- info6 %>% left_join(mktname, by = "DOI")
  
  info7 <- info6
  names(info7)[names(info7) == 'prod_s'] <- 'Brand'
  names(info7)[names(info7) == 'DOI'] <- 'Market'
  
  info8 <- info7[, c(
                     "Market",
                     "Brand",
                     "City",
                     "Date",
                     "Prod",
                     "Mkt",
                     'Share',
                     'Source')]  
  print(unique(info8$Market))
  print(unique(info8$Date))
  
  
  write.xlsx(
    info8,
    paste(
      "E:/MAX/Tide/validation/","ims_trend_1401-1906.xlsx",
      sep = ""
    )
  )
}


# max 趋势线
{
  
  
  #bed <- read.xlsx("I:/Eisai/MAX/6mkt/02_Inputs/100.xlsx")
  bed<-read.xlsx("E:/MAX/Tide/MODEL/凯时/000ref/universe.xlsx")
  
  alldata8000 <- NULL
  for (y in c('17','18','19')) {
    alldata8000_temp <- NULL
    for (i in c(2)) {
      print(MKT[[i]][2])
      # data20000 <-
    
      data20000 <- fread(paste(
        paste(
          "E:/MAX/Tide/MODEL/",MKT[[i]][2],"/060factorized/",
          MKT[[i]][2],
          "_Factorized_Units&Sales_WITH_OT",y, ".csv",
          sep = ""
        ),
        sep = ""
      ), header = T, stringsAsFactors = F)
      print(sum(data20000$f_sales))
      data20000$f_sales[data20000$f_sales<0]<-0
      print(sum(data20000$f_sales))
      print(unique(data20000$Date))
      print(sum(data20000$f_sales))
      # data20000 <- data20000_1[grepl('胶囊剂', data20000_1$Product) |
      #                            grepl('片剂', data20000_1$Product),]
      data20000$City[data20000$City %in% c("福州市", "厦门市", "泉州市")] <-
        "福厦泉市"
      data20000$City[data20000$City %in%
                       c("佛山市", "中山市", "东莞市", "珠海市")] <-
        "珠三角市"
      data8000 <- data20000 %>%
        filter(Panel_ID %in% bed$Panel_ID[bed$BEDSIZE>=100])
      data8000_cht <- data20000 %>%
        filter(Panel_ID %in% bed$Panel_ID[bed$BEDSIZE>=100])
      data8000_cht$City <- 'CHPA'
      
      data8000_2 <-
        group_by(data8000, City, Date, Product) %>%
        summarise_at(c("f_sales"), sum)
      data8000_3 <- group_by(data8000_2, City, Date) %>%
        summarise_at(c("f_sales"), sum)
      # data8000_2$City[Encoding(data8000_2$City) %in% 'unknown'] <- 
      #   iconv(data8000_2$City[Encoding(data8000_2$City) %in% 'unknown'], 
      #         to = 'UTF-8')
      # data8000_3$City[Encoding(data8000_3$City) %in% 'unknown'] <- 
      #   iconv(data8000_3$City[Encoding(data8000_3$City) %in% 'unknown'], 
      #         to = 'UTF-8')
      data8000_4 <-
        left_join(data8000_2, data8000_3, by = (c("City", "Date")))
      data8000_4$Share <- data8000_4$f_sales.x / data8000_4$f_sales.y
      data8000_4$Market <- MKT[[i]][2]
      alldata8000_temp <- rbind(alldata8000_temp, data8000_4)
    }
    alldata8000 <- rbind(alldata8000, alldata8000_temp)
  }
  
  
  # alldata8000$Product <- iconv(alldata8000$Product, to = "UTF-8")
  
  
  alldata8000$Product[Encoding(alldata8000$Product) %in% 'unknown'] <-
    iconv(alldata8000$Product[Encoding(alldata8000$Product) %in% 'unknown'],
          to = 'UTF-8')
  mapping <- koufu_pi
  names(mapping)
  mapping2 <- mapping[c('商品名_标准', "min2")] %>% 
    filter(!is.na(`商品名_标准`)) %>% unique()
  colnames(mapping2) <- c('商品名_标准', "最小产品单位_标准")
  alldata8000$Product <- gsub('NA', '', alldata8000$Product)
  alldata8000_2 <-
    left_join(alldata8000, mapping2, by = c("Product" = "最小产品单位_标准"))
  sum(is.na(alldata8000_2$商品名_标准))
  colnames(alldata8000_2)[colnames(alldata8000_2) == "商品名_标准"] <-
    "Brand"
  # alldata8000_2_v2 <- filter(alldata8000_2,!is.na(Brand))
  alldata8000_2_v2 <- alldata8000_2
  alldata8000_2_v2$Brand[is.na(alldata8000_2_v2$Brand)] <- 
    alldata8000_2_v2$Product[is.na(alldata8000_2_v2$Brand)]
  
  alldata8000_3 <- alldata8000_2_v2 %>%
    group_by(Market, Brand, City, Date) %>%
    summarise(Sales = sum(f_sales.x))
  alldata8000_4 <- alldata8000_2_v2 %>%
    group_by(Market, City, Date) %>%
    summarise(Sales = sum(f_sales.x))
  alldata8000_3$City[Encoding(alldata8000_3$City) %in% 'unknown'] <- 
    iconv(alldata8000_3$City[Encoding(alldata8000_3$City) %in% 'unknown'], 
          to = 'UTF-8')
  alldata8000_4$City[Encoding(alldata8000_4$City) %in% 'unknown'] <- 
    iconv(alldata8000_4$City[Encoding(alldata8000_4$City) %in% 'unknown'], 
          to = 'UTF-8')
  alldata8000_5 <-
    left_join(data.frame(alldata8000_3, stringsAsFactors = F), 
              data.frame(alldata8000_4, stringsAsFactors = F), 
              by = (c("Market", "City", "Date")))
  print(dim(alldata8000_3)[1] == dim(alldata8000_5)[1])
  alldata8000_5$Share <- alldata8000_5$Sales.x / alldata8000_5$Sales.y
  
  names(alldata8000_5)[names(alldata8000_5) == 'Sales.x'] <- 'Prod'
  names(alldata8000_5)[names(alldata8000_5) == 'Sales.y'] <- 'Mkt'
  print(sum(alldata8000_5$Prod) - sum(alldata8000$f_sales.x))
  alldata8000_5$Source <- 'MAX Share'
  
}

alldata8000_5$Date<-as.character(alldata8000_5$Date)
write.xlsx(alldata8000_5,
           paste('E:/MAX/Tide/validation/',MKT[[2]][2],'_max_trend_171819.xlsx',
                 sep = ""))



