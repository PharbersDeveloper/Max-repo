require(openxlsx)
require(stringi)
require(data.table)
require(sqldf)
require(tidyverse)

MKT <- list(c('凯纷','凯纷'))

prod_mapping <- read.xlsx("E:/MAX/Tide/Global/泰德产品匹配表.xlsx")

prod_mapping$Pack_ID <- stri_pad_left(prod_mapping$Pack_ID,7,'0')


ims_city_data <- fread('L:/全原盘数据/ssd2备份/MAX/Pfizer/Making ims flat file/201812/cn_IMS_Sales_Fdata_201812_1.txt',
                       header = T, stringsAsFactors = F) %>% filter(substr(Period_Code,1,4) %in% c(2018,2017))
ims_city_data_surplus <- fread('L:/全原盘数据/ssd2备份/MAX/Pfizer/Making ims flat file/201906/cn_IMS_Sales_Fdata_201906_1.txt',
                               header = T, stringsAsFactors = F) %>%
  filter((substr(Period_Code,1,4) %in% c(2018,2017) &(!Geography_id %in% unique(ims_city_data$Geography_id))) |
           substr(Period_Code,1,4) %in% 2019)

ims_city_data_all <- rbind(ims_city_data_surplus,ims_city_data)

table(ims_city_data_surplus$Geography_id,
      ims_city_data_surplus$Period_Code)

table(ims_city_data_all$Geography_id[substr(ims_city_data_all$Period_Code,1,4)==2018],
      ims_city_data_all$Period_Code[substr(ims_city_data_all$Period_Code,1,4)==2018])
setdiff(ims_city_data_all$Geography_id,ims_city_data_surplus$Geography_id)

ims_city_data_all$Pack_ID<-stri_pad_left(ims_city_data_all$Pack_ID,7,'0')
unique(ims_city_data_all$Period_Code)

unique(nchar(ims_city_data_all$Pack_ID))

ims_city_data_v2 <- ims_city_data_all
colnames(ims_city_data_v2)[colnames(ims_city_data_v2) == "Period_Code"] <- "Date"
ims_city_data_v2$Date <- gsub('M', '', ims_city_data_v2$Date)


info <- ims_city_data_v2 %>%
  filter(!(Geography_id %in% c('32C', "CHACACACACACAAA"))) %>%
  group_by(Date, Geography_id, Pack_ID) %>%
  summarise(Sales = sum(LC, na.rm = T))

###用通用名代替pack id生成info

ims_mol_id <- fread('L:/全原盘数据/ssd2备份/MAX/Pfizer/Making ims flat file/201906/cn_mol_ref_201906_1.txt',
                    header = T, stringsAsFactors = F)
names(ims_mol_id)

ims_mol_pack <- fread('L:/全原盘数据/ssd2备份/MAX/Pfizer/Making ims flat file/201906/cn_mol_lkp_201906_1.txt',
                      header = T, stringsAsFactors = F)
names(ims_mol_pack)

ims_prod <- fread('L:/全原盘数据/ssd2备份/MAX/Pfizer/Making ims flat file/201906/cn_prod_ref_201906_1.txt',
                  header = T, stringsAsFactors = F)
names(ims_prod)


ims_mol_f <- left_join(ims_mol_pack,ims_mol_id,
                       by = c('Molecule_ID'='Molecule_Id')) %>%
  left_join(ims_prod[,c('Pack_Id','Prd_desc')],
            by = c('Pack_ID'='Pack_Id'))

nrow(ims_mol_f)==nrow(ims_mol_pack)

ims_mol <- ims_mol_f %>% 
  arrange(Molecule_Desc) %>%
  group_by(Pack_ID=stri_pad_left(Pack_ID,7,'0'),Prd_desc) %>%
  summarise(Molecule_Desc=paste0(Molecule_Desc,collapse = '+'))

info1 <- left_join(info, ims_mol[,c('Pack_ID','Molecule_Desc','Prd_desc')]%>% unique(), 
                   by = c("Pack_ID"='Pack_ID')) 

nrow(info1)==nrow(info)

info1$Prd_desc[info1$Prd_desc %in% c('JIA LUO NING       JYF')] <- '加罗宁' 
info1$Prd_desc[info1$Prd_desc %in% c('KAI FEN            BAI')] <- '凯纷' 
info1$Prd_desc[info1$Prd_desc %in% c('NUO YANG           JSH')] <- '诺扬'
info1$Prd_desc[info1$Prd_desc %in% c('DYNASTAT           PFZ')] <- '特耐'
info1$Prd_desc[info1$Prd_desc %in% c('ZEPOLAS            BAI')] <- '氟比洛芬'
info1$Prd_desc[info1$Prd_desc %in% c('DE BAI AN          BAI')] <- '氟比洛芬'

info1_v1 <- info1 %>% 
  filter(Molecule_Desc %in% c('PROPACETAMOL','DEZOCINE','MORPHINE','FLURBIPROFEN',
                              'KETOROLAC','BUTORPHANOL','TRAMADOL','LORNOXICAM',
                              'PENTAZOCINE','PARECOXIB','FLURBIPROFEN AXETIL')) %>%
  group_by(Date, Geography_id , Pack_ID,Molecule_Desc,Prd_desc,mkt='凯纷') %>%
  summarise(ims_poi_vol = sum(Sales, na.rm = T))

city_cname <- read.xlsx("L:/全原盘数据/F盘备份/LJX/Eisai/02_Inputs/ims_cname.xlsx")
info2 <- left_join(info1_v1, city_cname, by = c("Geography_id" = "GEO_CD"))

unique(info2$Geography_id[is.na(info2$City)])
info2$City[is.na(info2$City)] <- 'CHPA'
info3 <- info2# %>% left_join(unique(koufu_pi[,c('pfc','商品名_标准')]),by=c('Pack_ID'='pfc'))
#info3$商品名_标准[is.na(info3$商品名_标准)] <- 'other'
print(sum(info2$ims_poi_vol[info2$City %in% 'CHPA' & 
                              substr(info2$Date, 4, 4) %in% '8']))


# IMS info

y = '8'
{
  
  info3_v1 <- filter(info3, substr(Date, 4, 4) %in% y)
  
  info3_v1$DOI <- info3_v1$mkt
  
  print(table(info3_v1$Date))
  
  ims_info <- group_by(info3_v1, 
                       DOI, City, prod_s=Prd_desc,Pack_ID) %>%
    summarise(ims_poi_vol = sum(ims_poi_vol))
  ims_info1 <- group_by(ims_info, DOI, City) %>%
    summarise(ims_mkt_vol = sum(ims_poi_vol))
  ims_info2 <- left_join(ims_info, ims_info1, by = c('DOI', 'City'))
  ims_info2$ims_share <- ims_info2$ims_poi_vol / ims_info2$ims_mkt_vol
  # unique(ims_info2$prod_s[ims_info2$DOI%in%MKT[[mkt]][1]&ims_info2$City%in%c('CHPA')])
  for(mkt in 1:length(MKT)) {
    print(MKT[[mkt]][1])
    # ims_info3 <- ims_info2[ims_info2$DOI %in% MKT[[mkt]][1] & 
    #                          ims_info2$prod_s %in% 
    #                          c('他莫昔芬', '特茉芬', '枢瑞', '法乐通'), 
    #                        c('City', 'prod_s', 'ims_poi_vol', 'ims_share')]
    ims_info3 <- ims_info2[ims_info2$DOI %in% MKT[[mkt]][1], 
                           c('City', 'prod_s','ims_poi_vol', 'ims_share','Pack_ID')]
    names(ims_info3) <- c('city', 'poi','ims_poi_vol', 'ims_share','Pack_ID')
    write.xlsx(
      ims_info3,
      paste(
        "E:/MAX/Tide/MODEL/",MKT[[mkt]][1],"/011IMS_data/",
        MKT[[mkt]][1],"_ims_info1", y, ".xlsx",
        sep = ""
      )
    )
  }
}

# IMS 趋势线
{
  info3_1 <- info3#[info3$City %in% c('北京市','上海市','广州市'),]
  info3_1$DOI <- info3_1$mkt
  names(info3_1)[names(info3_1)== 'Prd_desc'] <- 'prod_s'
  
  info4 <- info3_1 %>%
    group_by(DOI, City, Date, prod_s,Pack_ID) %>%
    summarise(IMS_Sales = sum(ims_poi_vol))
  info5 <- info3_1 %>%
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
  
  info8 <- info7[, c("Market",
                     "Brand",
                     "City",
                     "Date",
                     "Prod",
                     "Mkt",
                     'Share',
                     'Source',
                     'Pack_ID')] 
  print(unique(info8$Market))
  print(unique(info8$Date))
  # colnames(info8) <- c("Market", "Brand", "City", "Date",
  #                      "Prod", "Mkt", 'Share', 'Source')
  # print(all(colnames(info8) == colnames(alldata8000_5)))
  # print(sum(info8$Prod) == sum(info1_v1$ims_poi_vol))
  write.xlsx(
    info8,
    paste(
      "E:/MAX/Tide/MODEL/凯纷/011IMS_data/ims_trend_v2.xlsx",
      sep = ""
    )
  )
}
pp<- read.xlsx('E:/MAX/Tide/MODEL/凯纷/000ref/universe.xlsx')

bed <- pp$Panel_ID[pp$BEDSIZE > 99]

# max 趋势线

{
  alldata8000 <- NULL
  for (y in c('17','18','19')) {
    alldata8000_temp <- NULL
    for (i in c(1)) {
      print(MKT[[i]][2])
      # data20000 <-chk
      data20000 <- fread(paste(
        paste(
          "L:/MAX result/Tide/1906/",
          MKT[[i]][2],
          '_Factorized_Units&Sales_WITH_OT', y, '.csv',
          sep = ""
        ),
        sep = ""
      ), header = T, stringsAsFactors = F)
      # data20000 <- data20000[!grepl('粉针|注射',data20000$Product),]
      print(y)
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
        filter(Panel_ID %in% bed)
      data8000_cht <- data20000 %>%
        filter(Panel_ID %in% bed)
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
  
  
  # alldata8000$Product[Encoding(alldata8000$Product) %in% 'unknown'] <-
  #   iconv(alldata8000$Product[Encoding(alldata8000$Product) %in% 'unknown'],
  #         to = 'UTF-8')
  mapping <- prod_mapping
  names(mapping)
  mapping2 <- unique(mapping[c('标准商品名', "min2")]) # %>% 
  # filter(!is.na(`商品名_标准`)) %>% unique()
  colnames(mapping2) <- c('标准商品名', "最小产品单位_标准")
  #alldata8000$Product <- gsub('NA', '', alldata8000$Product)
  alldata8000_2 <-
    left_join(alldata8000, mapping2, by = c("Product" = "最小产品单位_标准"))
  print(nrow(alldata8000_2)==nrow(alldata8000))
  sum(is.na(alldata8000_2$标准商品名))
  colnames(alldata8000_2)[colnames(alldata8000_2) == "标准商品名"] <-
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
  # alldata8000_3$City[Encoding(alldata8000_3$City) %in% 'unknown'] <- 
  #   iconv(alldata8000_3$City[Encoding(alldata8000_3$City) %in% 'unknown'], 
  #         to = 'UTF-8')
  # alldata8000_4$City[Encoding(alldata8000_4$City) %in% 'unknown'] <- 
  #   iconv(alldata8000_4$City[Encoding(alldata8000_4$City) %in% 'unknown'], 
  #         to = 'UTF-8')
  alldata8000_5 <-
    left_join(data.frame(alldata8000_3, stringsAsFactors = F), 
              data.frame(alldata8000_4, stringsAsFactors = F), 
              by = (c("Market", "City", "Date")))
  print(dim(alldata8000_3)[1] == dim(alldata8000_5)[1])
  alldata8000_5$Share <- alldata8000_5$Sales.x / alldata8000_5$Sales.y
  
  names(alldata8000_5)[names(alldata8000_5) == 'Sales.x'] <- 'Prod'
  names(alldata8000_5)[names(alldata8000_5) == 'Sales.y'] <- 'Mkt'
  print(sum(alldata8000_5$Prod,na.rm = T) - sum(alldata8000$f_sales.x,na.rm = T))
  alldata8000_5$Source <- 'MAX Share'
  alldata8000_5$Date<-as.character(alldata8000_5$Date)
  
}


write.xlsx(alldata8000_5,
           paste("E:/MAX/Tide/MODEL/凯纷/011IMS_data/",MKT[[i]][2],
                 "_max_trend_171819.xlsx",
                 sep = "")) 
# 
# write.xlsx(alldata8000_5,
#            paste("E:/MAX/Amgen/UPDATE/1812/LD_max_trend_18.xlsx",
#                  sep = ""))

