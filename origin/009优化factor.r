require(data.table)
require(openxlsx)

require(feather)
require(openxlsx)

require(stringi)

require(RODBC)
require(feather)
require(data.table)
require(tidyverse)
# install.packages('CVXR')
require(CVXR)


# mktl<-c('RCC','HCC','WH')


mkt='凯纷'

path <- 'E:/MAX/Tide/MODEL/'


univer <- read.xlsx('E:/MAX/Tide/MODEL/凯纷/000ref/universe.xlsx')#####universe文件


ims<-read.xlsx(paste0("E:/MAX/Tide/MODEL/",mkt,"/011IMS_data/",mkt,"_ims_info18.xlsx"))
names(ims)

names(ims)[names(ims)=='city']='City'

#       stringsAsFactors = F)##################读取MAX数据


# max <- chk


max<-fread(paste0('E:/MAX/Tide/MODEL/凯纷/060factorized/',mkt,'_Factorized_Units&Sales_WITH_OT18.csv'),
           stringsAsFactors = F)

##################读取MAX数据

max1<-max[max$Panel_ID %in% univer$Panel_ID[univer$BEDSIZE>99]&
            substr(max$Date,1,4)%in%c('2018'),]

sum(max1$f_sales)
###########床位100以上的医院

#%>% left_join(unique(univer[,c('City','Province')]),by=c('City'='City')) 
#unique(max1$City[is.na(max1$Province)])


max1$Citynew<-max1$City
max1$Citynew[max1$City %in% c("福州市","厦门市","泉州市")] <-
  "福厦泉市"
max1$Citynew[max1$City %in%
               c("佛山市","中山市","东莞市","珠海市")] <-
  "珠三角市"
# write.xlsx(unique(max1$Citynew),'H:/qlwang/city1.xlsx')
# max1$Citynew[max1$Province %in% c("浙江") & !(max1$City %in% c("杭州市","宁波市","温州市"))] <-"浙江省"

max1$Citynew[!max1$Citynew %in%unique(ims$City)]<-'other'
unique(max1$Citynew)

# max1$brand<-max1$Product
# max1$brand[!max1$Product%in% htn_cpa]<- 'OTHERS'

pipei <- read.xlsx("E:/MAX/Tide/Global/泰德产品匹配表.xlsx")
names(pipei)
pipei1 <- unique(pipei[,c('标准商品名','标准剂型',
                          '标准规格',
                          'Pack_number','标准生产厂家','min2')])
names(pipei1) <- c('商品名_标准','剂型_标准',
                   '规格_标准',
                   '包装数量_标准','生产企业_标准','min2')


max1_v <-left_join(max1,pipei1,by =c('Product'='min2'))
nrow(max1_v) == nrow(max1)

sum(is.na(max1_v$商品名_标准))

names(max1_v)
names(max1_v)[names(max1_v) == '商品名_标准'] <-'brand'


########读取panel数据

######读取匹城市文件
{
  #mktlist<-c('RCC','HCC','WH')
  mktlist<-c('凯纷')
  
  bll<-read.xlsx(paste0('E:/MAX/Tide/MODEL/凯纷/010Panel_data/凯纷_Panel_2018.xlsx'))
  # bll<-bll[substr(bll$Date,1,4)%in%c('2017'),]
  #bll$Brand[bll$Brand %in%c('康忻')]<-'康昕'
  # unique(bll$DOI)
  
  bll1<-left_join(bll,univer[univer$BEDSIZE>99 &univer$PANEL==1,
                             c('Panel_ID','City','Province')],
                  by=c('HOSP_ID'='Panel_ID')) %>%
    filter(!is.na(City)) #%>% left_join(koufu_pi1,by=c('Strength'='min2')) 
  
  unique(bll1$HOSP_ID[is.na(bll1$City)])
  
  bll1$Citynew <- bll1$City
  bll1$Citynew[bll1$City %in% c("福州市","厦门市","泉州市")] <-
    "福厦泉市"
  bll1$Citynew[bll1$City %in%
                 c("佛山市","中山市","东莞市","珠海市")] <-
    "珠三角市"
  # bll1$Citynew[bll1$Province %in% c("浙江") & !(bll1$City %in% c("杭州市","宁波市","温州市"))] <-"浙江省"
  bll1$Citynew[!bll1$Citynew %in%unique(ims$City)]<-'other'
  
  # bll1$Brand<-max1$Product
  # bll1$Brand[!bll1$Brand%in% htn_cpa]<- 'OTHERS'
  
  # bll1$Brand[!(bll1$brand1 %in% inner_brand)] <- '其他'
  names(bll1)
  
  bll1_v <- left_join(bll1,pipei1,by =c('Prod_Name'='min2'))
  nrow(bll1_v) == nrow(bll1)
  
  sum(is.na(bll1_v$商品名_标准))
  # bll1$Brand<-max1$Product
  
  
  # bll1$Brand[!(bll1$brand1 %in% inner_brand)] <- '其他'
  names(bll1_v)
  names(bll1_v)[names(bll1_v) == '商品名_标准'] <- 'Brand'
  
  bll2<-group_by(bll1_v,mkt='凯纷',Brand,City=Citynew) %>% 
    summarise(panel_Sales=sum(Sales)) #####城市的产品层面
  
  bll3<-group_by(bll1_v,Market1='凯纷',City1=Citynew) %>% 
    summarise(panel_mkt=sum(Sales)) #####全国的市场
  
}


#####MAX数据
{
  
  # max1$Brand[!(max1$brand1 %in% inner_brand)] <- '其他'
  max2<-group_by(max1_v,mkt='凯纷',brand,City=Citynew) %>% 
    summarise(max_Prod=sum(f_sales)) #####城市的产品层面
  
  max3<-group_by(max1_v,Market1='凯纷',City1=Citynew) %>% 
    summarise(max_mkt=sum(f_sales)) #####全国的市场
  
  
}
# names(ims_v1_otherall)[names(ims_v1_otherall)=='Prod']<-'ims_Prod'


##确定IMS三个最大的产品
{
  #######读取IMS数据

  ims$Market = mkt
  ims_inner <-
    ims # %>% filter(!(poi %in% c('TRESIBA', '万苏林|', '胰岛素|', '优乐灵|',
  #                             'TRES')))#######去除IMS数据中商品的交集
  #######去除IMS数据中商品的交集
  # ims_inner$Brand[!(ims_inner$brand1 %in% inner_brand)] <- '其他'
  names(ims_inner)[names(ims_inner) == 'poi'] <- 'Brand'
  names(ims_inner)[names(ims_inner) == 'ims_poi_vol'] <- 'Prod'
  
  
  ims_v1<-ims_inner %>% group_by(Market,Brand,City) %>% summarise(Prod=sum(Prod))
  
  # ims_v1$City1[ims_v1$City1 %in% c('浙江市')]<-'浙江省'
  # ims_v1$City1[ims_v1$City1 %in% c('福夏泉市')]<-'福厦泉市'
  # ims_v1$City1[ims_v1$City1 %in% c('珠三角')]<-'珠三角市'
  # unique(ims_v1$City1)
  
  ims_v1_part1<-ims_v1[ims_v1$City %in% c('CHPA'),] ##########市场与全国层面的市场值
  ims_v1_part2<-ims_v1[!ims_v1$City %in% c('CHPA'),]#######市场与城市层面的市场值
  sum(ims_v1_part1$Prod,ims_v1_part2$Prod)-sum(ims_v1$Prod)######验证
  
  ims_v1_part1_v1<-group_by(ims_v1_part1,Market,Brand) %>% 
    summarise(Prod=sum(Prod))##### 全国的商品的销量
  ims_v1_part2_v1<-group_by(ims_v1_part2,Market,Brand) %>% 
    summarise(Prod=sum(Prod))#####other之外省份商品的销量
  dim(ims_v1_part1_v1)   
  dim(ims_v1_part2_v1)
  
  ims_v1_left<-left_join(ims_v1_part1_v1,ims_v1_part2_v1,by=c('Market','Brand'))
  ims_v1_left$Prod.y[is.na(ims_v1_left$Prod.y)]<-0
  ims_v1_left$Prod.other<-ims_v1_left$Prod.x-ims_v1_left$Prod.y
  ims_v1_left$City<-'other'
  names(ims_v1_left)<-c('Market','Brand','全国','14个城市','Prod','City')
  ims_v1_left1<-ims_v1_left[,c(1,2,6,5)]
  
  ims_v1_otherall<-rbind(ims_v1_part2,ims_v1_left1) %>% arrange(Market,City,desc(Prod)) %>%
    group_by(Market,City) %>% mutate(n=row_number()) %>% filter(n<4)
  
  
  # ims_v1_otherall_firstconsider<-rbind(ims_v1_part2,ims_v1_left1) %>% 
  #   filter(Brand %in%c('代文','倍博特'))
  # ims_v1_otherall_firstconsider$n<-2
  # ims_v1_otherall_othertop1<-rbind(ims_v1_part2,ims_v1_left1) %>% 
  #   filter(!Brand %in%c('代文','倍博特','OTHERS'))%>%
  #   arrange(Market,City,desc(Prod)) %>%
  #   group_by(Market,City) %>% mutate(n=row_number()) %>% filter(n<2)
  # 
  # 
  # ims_v1_otherall<-rbind(ims_v1_otherall_othertop1,
  #                        ims_v1_otherall_firstconsider)
  # ims_v1_otherall$n[ims_v1_otherall$Brand %in% c('倍博特')]<-3
  names(ims_v1_otherall)[names(ims_v1_otherall)=='Prod']<-'ims_Prod'
  #write.xlsx(ims_v1_otherall,"H:/qlwang/诺华/诺华MAX/HTN市场城市商品top3.xlsx")
  # names(ims_v1_otherall)[names(ims_v1_otherall)=='Prod']<-'ims_Prod'
  # sum(ims_v1_otherall$Prod)-sum(ims_v1_part1$Prod)
  city<-unique(ims_v1_otherall$City)
  
  ims_v1_mkt<-rbind(ims_v1_part2,ims_v1_left1) %>% 
    group_by(Market,City) %>% summarise(ims_mkt=sum(Prod))
  unique(ims_v1_mkt$City)
}

setdiff(city,unique(max1$Citynew))
setdiff(city,unique(bll1$Citynew))

ims_panel_max <- ims_v1_otherall %>%
  left_join(max2, by = c(
    "Market" = 'mkt',
    'Brand' = 'brand',
    'City' = 'City'
  )) %>%
  left_join(bll2, by = c('Market' = 'mkt', 'Brand', 'City' = 'City')) %>%
  gather(cate, prd, -Market, -Brand, -City, -n, na.rm = T) %>%
  select(-Brand) %>%
  unite(new, cate, n) %>% spread(new, prd, fill = 0) %>% 
  left_join(ims_v1_mkt,by=c('Market','City')) %>% 
  left_join(max3,by=c('Market'='Market1','City'='City1')) %>% 
  left_join(bll3,by=c('Market'='Market1','City'='City1'))


ims_panel_max[,3:14][is.na(ims_panel_max[,3:14])]<-0

# ims_panel_max$City1[ims_panel_max$Market1 %in% c('WH')]
# setdiff(ims_panel_max$City1[ims_panel_max$Market1 %in% c('WH')],ims_panel_max$City1[ims_panel_max$Market1 %in% c('RCC')]
# ims_panel_max$City1[ims_panel_max$Market1 %in% c('HCC')]



ims_panel_max1=data.frame(ims_panel_max,stringsAsFactors = F)
dim(ims_panel_max1)[1]
qq=sapply(1:dim(ims_panel_max1)[1], function(i){
  f=Variable()
  p=Problem(Minimize(max(abs((f*(ims_panel_max1[i,'max_Prod_1']-
                                   ims_panel_max1[i,'panel_Sales_1'])+
                                ims_panel_max1[i,'panel_Sales_1'])/ims_panel_max1[i,'ims_Prod_1']-1),
                         abs((f*(ims_panel_max1[i,'max_Prod_2']-
                                   ims_panel_max1[i,'panel_Sales_2'])+
                                ims_panel_max1[i,'panel_Sales_2'])/ims_panel_max1[i,'ims_Prod_2']-1),
                         abs((f*(ims_panel_max1[i,'max_Prod_3']-
                                   ims_panel_max1[i,'panel_Sales_3'])+
                                ims_panel_max1[i,'panel_Sales_3'])/ims_panel_max1[i,'ims_Prod_3']-1),
                         abs((f*(ims_panel_max1[i,'max_mkt']-
                                   ims_panel_max1[i,'panel_mkt'])+
                                ims_panel_max1[i,'panel_mkt'])/ims_panel_max1[i,'ims_mkt']-1))),
            list(abs((f*(ims_panel_max1[i,'max_mkt']-ims_panel_max1[i,'panel_mkt'])+
                        ims_panel_max1[i,'panel_mkt'])/ims_panel_max1[i,'ims_mkt']-1)<0.05))
  result=psolve(p)
  print(i)
  print(result$getValue(f))
  print(max(abs((result$getValue(f)*(ims_panel_max1[i,'max_Prod_1']-
                                       ims_panel_max1[i,'panel_Sales_1'])+
                   ims_panel_max1[i,'panel_Sales_1'])/ims_panel_max1[i,'ims_Prod_1']-1),
            abs((result$getValue(f)*(ims_panel_max1[i,'max_Prod_2']-
                                       ims_panel_max1[i,'panel_Sales_2'])+
                   ims_panel_max1[i,'panel_Sales_2'])/ims_panel_max1[i,'ims_Prod_2']-1),
            abs((result$getValue(f)*(ims_panel_max1[i,'max_Prod_3']-
                                       ims_panel_max1[i,'panel_Sales_3'])+
                   ims_panel_max1[i,'panel_Sales_3'])/ims_panel_max1[i,'ims_Prod_3']-1),
            abs((result$getValue(f)*(ims_panel_max1[i,'max_mkt']-
                                       ims_panel_max1[i,'panel_mkt'])+
                   ims_panel_max1[i,'panel_mkt'])/ims_panel_max1[i,'ims_mkt']-1)))
  return(result$getValue(f))
})



ims_panel_max1$factor<-qq

ims_panel_max1$factor[is.na(ims_panel_max1$factor)]<-0
# ims_panel_max1$factor[ims_panel_max1$factor<0]=0
# ims_panel_max1$factor[ims_panel_max1$factor>2]=2
sum(is.na(ims_panel_max1$factor))
ims_panel_max1$gap1 <-
  (
    ims_panel_max1$factor * (ims_panel_max1$max_Prod_1 -
                               ims_panel_max1$panel_Sales_1) +
      ims_panel_max1$panel_Sales_1
  ) / ims_panel_max1$ims_Prod_1 - 1
ims_panel_max1$gap2 <-
  (
    ims_panel_max1$factor * (ims_panel_max1$max_Prod_2 -
                               ims_panel_max1$panel_Sales_2) +
      ims_panel_max1$panel_Sales_2
  ) / ims_panel_max1$ims_Prod_2 - 1
ims_panel_max1$gap3 <-
  (
    ims_panel_max1$factor * (ims_panel_max1$max_Prod_3 -
                               ims_panel_max1$panel_Sales_3) +
      ims_panel_max1$panel_Sales_3
  ) / ims_panel_max1$ims_Prod_3 - 1
ims_panel_max1$gap_mkt <-
  (
    ims_panel_max1$factor * (ims_panel_max1$max_mkt -
                               ims_panel_max1$panel_mkt) +
      ims_panel_max1$panel_mkt
  ) / ims_panel_max1$ims_mkt - 1

# write.xlsx(ims_panel_max1,"J:/MAX/BMS/Model/Chemo/050factor/On_factor&city_full_optim.xlsx")


# city_map<-read.xlsx("H:/qlwang/诺华/诺华MAX/IMS与MAX城市匹配_OAD.xlsx")

# ims_panel_max2<-left_join(ims_panel_max1,city_map,by=c('City'='City2'))

write.xlsx(ims_v1_otherall,'E:/MAX/Tide/MODEL/凯纷/050factor/凯纷_前三产品具体名称.xlsx')

write.xlsx(ims_panel_max1,'E:/MAX/Tide/MODEL/凯纷/050factor/凯纷_新版factor的gap.xlsx')

write.csv(unique(ims_panel_max1[,c('City','factor')]),
          'E:/MAX/Tide/MODEL/凯纷/050factor/凯纷_factor&city_full_2.csv',row.names = F)

