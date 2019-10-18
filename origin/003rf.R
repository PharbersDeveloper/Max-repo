require(openxlsx)
require(dplyr)
require(tidyr)
require(stringi)
require(stringr)
require(data.table)
require(randomForest)
mkt <- '凯纷'

hosp.range <- read.xlsx("E:/MAX/Tide/MODEL/凯纷/000ref/universe.xlsx")

all.hosp <- unique(hosp.range$Panel_ID[hosp.range$BEDSIZE >99])
hosp.range.sample <- unique(hosp.range$Panel_ID[hosp.range$PANEL == 1 & 
                                                  hosp.range$BEDSIZE > 99])

rawdata <- read.xlsx(paste0("E:/MAX/Tide/MODEL/凯纷/010Panel_data/凯纷_Panel_2018.xlsx"))

####补全PHA.ID
#rawdata1 <- left_join(rawdata, gyc_pha[,c(1,3)], by = c('ID' = 'GYC'))
rawdata.m <- rawdata
names(rawdata.m)
names(rawdata.m)[names(rawdata.m)=='HOSP_ID'] <- 'PHA.ID'
####得到因变量
rawdata.i <- rawdata.m[rawdata.m$PHA.ID %in% 
                         hosp.range$Panel_ID[hosp.range$PANEL == 1], ] %>% 
  group_by(PHA.ID, DOI) %>% summarise(Sales = sum(Sales, na.rm = T))


moh.bt <- read.xlsx('k:/LJX/安斯泰来医院匹配/去重3.xlsx',
                    sheet = '补充码小的替换100床位以下的') ####BT与PHA的匹配

moh.bt.m<-group_by(moh.bt[,c(1,3)],PHA) %>% summarise(BT=first(BT))
# 
# write.xlsx(moh_bt1,'H:/qlwang/齐鲁/1.xlsx')
###读取科室信息
doctor <- read.xlsx('k:/LJX/安斯泰来医院匹配/副本医院潜力+-+医院范围，重点科室+医生数_ljx.xlsx')
doctor.m<-doctor[!is.na(doctor$BT_Code),c(4:10)]
names(doctor.m)
doctor.g<-gather(doctor.m,dr,no,-c(BT_Code,Department)) %>% 
  unite(dr1, c(Department, dr)) %>% 
  group_by(BT_Code,dr1) %>%                                                         
  summarise(no = sum(no, na.rm = T)) %>%
  spread(dr1, no, fill = 0)

###读取ind1表
ind <- fread('L:/全原盘数据/ssd2备份/qlwang/齐鲁/ind1.csv',
             stringsAsFactors = F)
###筛选床位数大于100的，并与moh_bt相连接


####以前版本的PHA0012297重复出现

ind.m <- ind[ind$PHA.ID %in% hosp.range$Panel_ID,] %>% data.frame(stringsAsFactors = F)

a<-NULL
b<-NULL
for (j in c(1:215)){
  print (j)
  a[j]<-sum(!is.na(ind.m[,j]))
  b[j]<-paste(j,a[j],sep="-")
}

b<-as.data.frame(b)

# information,c("person","grade","score"), sep = "-")
c<-separate(b,b,c('列数','非缺失值'),sep="-")
c$列数<-as.numeric(c$列数)
c$非缺失值<-as.numeric(c$非缺失值)
c1<-c$列数[c$非缺失值>=15000 & !(c$列数 %in% c(1:8))] 
c2<-names(ind.m[,c1])
c3<-c2[!grepl('机构|省|市|县|医院级别|医院等次|医院类型|性质|地址|邮编|年诊疗|总收入|门诊药品收入|住院药品收入|总支出',
              c2)]#####去掉无用列



ind2 <- ind.m[,c('PHA.ID',
                 names(ind.m)[names(ind.m) %in% c3],
                 names(ind.m)[grepl('心血管',names(ind.m))])]

ind3<-left_join(ind2,moh.bt.m,by=c('PHA.ID'='PHA')) 

ind4<-left_join(ind3,doctor.g,by=c('BT'='BT_Code'))

# ind5<-ind4[,c(names(ind4)[names(ind4) %in% names(ind2)],
#               names(ind4)[grepl('_Dr_N_',names(ind4))])]

ind5<-ind4[,c(names(ind4)[names(ind4) %in% names(ind2)],
              names(ind4)[grepl('_Dr_N_',names(ind4))])]

#unique(ind5$Province)

names(ind5)

# 下标可能要改
ind5[, 12:length(ind5)][is.na(ind5[, 12:length(ind5)])] <- 0
sum(is.na(ind5[, 12:length(ind5)]))
sum(is.na(ind5$Est_DrugIncome_RMB[ind2$PHA.ID %in% all.hosp]))

modeldata <- left_join(ind5, rawdata.i, by = 'PHA.ID')
sum(!is.na(modeldata$Sales))
modeldata$flag_model <- modeldata$PHA.ID %in% 
  hosp.range$Panel_ID[hosp.range$PANEL == 1]
sum(modeldata$flag_model)
modeldata$Sales[is.na(modeldata$Sales) & modeldata$flag_model] <- 0

f1=function(x){
  y=(x+0.001)^(1/2)
  # y=log(x+0.001, base=2)
  # y=x
  # y=log(x+0.001)
  return(y)
}
f2=function(x){
  y=x^(2)-0.001
  # y=2^(x) - 0.001
  # y=x
  # y= exp(x) - 0.001
  return(y)
}
modeldata$v = ifelse(modeldata$Sales > 0, f1(modeldata$Sales), 0)
modeldata$Hosp_level <- as.factor(modeldata$Hosp_level)
modeldata$Region <- as.factor(modeldata$Region)
modeldata$City.Tier.2010 <- as.factor(modeldata$City.Tier.2010)
modeldata$respailty<-as.factor(modeldata$respailty)
modeldata$Re.Speialty <- as.factor(modeldata$Re.Speialty)


outlier <- NULL


# sum(is.na(trn$v))

trn <- modeldata[modeldata$PHA.ID %in% 
                   hosp.range$Panel_ID[hosp.range$PANEL == 1] & 
                   !(modeldata$PHA.ID %in% outlier),]
# tst <- modeldata[!modeldata$flag_model | (modeldata$PHA.ID %in% outlier),]
names(trn)
ntree=500;mtry=8
set.seed(10)
#########所用自变量个数：64个自变量
v_index <- c(2:4,7,10,12:131,135)
A=randomForest(v~.,data=trn[,v_index],
               importance=TRUE,proximity=TRUE,
               ntree=ntree,mtry=mtry)
chk <- A$importance[order(-A$importance[,2]),]

# 这个变量显示了各个变量的重要程度
print(chk)

CV <- function(n, Z = 10, seed = 1000) {
  z = rep(1:Z, ceiling(n/Z))[1:n]
  set.seed(seed);z=sample(z, n)
  mm = list()
  for(i in 1:Z) mm[[i]] = (1:n)[z==i]
  return(mm)
}

w = data.frame(trn, 
               stringsAsFactors = F)
K = 5
mm = CV(nrow(w), Z=K, seed = 3)


##以下为各个省的NMSE

cll<-NULL
err<-NULL
NMSE=rep(0,K);NMSE0=NMSE
for(i in 1:K){
  #i=1
  print(i)
  m = mm[[i]]
  A2 = randomForest(v~.,w[-m,v_index],importance=TRUE,proximity=TRUE,
                    ntree=ntree,mtry=mtry)
  w0=w[-m,]#####训练集数据
  w1=w[m,]######测试集数据
  w0$y0=f2(predict(A2,w0))########训练集的预测值
  w0$y0_=f2(w$v[-m])##########训练集的真实值
  
  
  w1$y1=f2(predict(A2,w1))#########测试集的预测值
  w1$y1_=f2(w$v[m])###########测试集的真实值
  
  ##########(真实值减去预测值)的平方 除 (真实值减去mean(真实值))的平方
  a0<-group_by(w0,Province) %>% 
    summarise(NMSE0=mean((y0_-y0)^2)/mean((y0_-mean(y0_))^2))
  a0$组<-i
  a1<-group_by(w1,Province) %>% 
    summarise(NMSE=mean((y1_-y1)^2)/mean((y1_-mean(y1_))^2))
  a1$组<-i
  c<-left_join(a0,a1,by=c('Province','组'))
  cll<-rbind(cll,c)
  
  y0_c=f2(predict(A2,w[-m,]))
  y1_c=f2(predict(A2,w[m,]))
  
  NMSE0[i]=mean((f2(w$v[-m])-y0_c)^2)/mean((f2(w$v[-m])-mean(f2(w$v[-m])))^2)
  NMSE[i]=mean((f2(w$v[m])-y1_c)^2)/mean((f2(w$v[m])-mean(f2(w$v[m])))^2)
  
  err <- rbind(err, data.frame(NMSE0=NMSE0[i],NMSE = NMSE[i]))
  
}

cll1<-cll %>% gather(lcat,num,-Province,-组) %>% mutate(cat=paste0(lcat,组)) %>%
  select(-组,-lcat) %>% spread(cat,num)

write.xlsx(cll1,'E:/MAX/Tide/MODEL/凯纷/051RF/误差.xlsx')



# E = NULL
NMSE=rep(0,K);NMSE0=NMSE
for(i in 1:K){
  m = mm[[i]]
  A2 = randomForest(v~.,w[-m, v_index],importance=TRUE,proximity=TRUE,
                    ntree=ntree,mtry=mtry)
  y0=f2(predict(A2,w[-m,]))
  y1=f2(predict(A2,w[m,]))
  NMSE0[i]=mean((f2(w$v[-m])-y0)^2)/mean((f2(w$v[-m])-mean(f2(w$v[-m])))^2)
  NMSE[i]=mean((f2(w$v[m])-y1)^2)/mean((f2(w$v[m])-mean(f2(w$v[m])))^2)
}

# 以下两个输出分别是模型在训练集和测试集上的误差
# 训练集
print(NMSE0)
# 测试集
print(NMSE)
mean(NMSE0)
mean(NMSE)

# outlier挑选
if(FALSE){
  i = 1
  m = mm[[i]]
  # M = mean((w[m, 'drug2016'] - mean(w[m, 'drug2016'], na.rm = T))^2)
  # a = kknn(drug2016 ~., 
  #          w[-m,-c(1,2)], 
  #          w[m,-c(1,2)],
  #          k=3)
  A2 = randomForest(v~.,w[-m, v_index],importance=TRUE,proximity=TRUE,
                    ntree=ntree,mtry=mtry)
  # E = c(E, mean((w[m, 'drug2016'] - a$fitted.values)^2)/M)
  y0=f2(predict(A2,w[-m,]))
  y1=f2(predict(A2,w[m,]))
  chk=mean((f2(w$v[-m])-y0)^2)/mean((f2(w$v[-m])-mean(f2(w$v[-m])))^2)
  chk1=mean((f2(w$v[m])-y1)^2)/mean((f2(w$v[m])-mean(f2(w$v[m])))^2)
  print(chk)
  print(chk1)
  a = w[m, ]
  a$result <- y1
  a$MAT <- f2(a$v)
  write.xlsx(a,'E:/MAX/Beite/MODEL/Beite/030ot_input/ot_2.xlsx')
}

result <- data.frame(modeldata)
result$DOI <- '凯纷'
result$outlier <- ifelse(result$PHA.ID %in% outlier, 1, 0)
result$sales_from_model <- f2(predict(A, result))
sum(result$sales_from_model)
result$training_set <- ifelse(result$PHA.ID %in% trn$PHA.ID, 1, 0)
result$final_sales <- ifelse(result$flag_model, result$Sales, 
                             result$sales_from_model)
sum(result$final_sales[result$PHA.ID %in% all.hosp]) ######9800

sum(result$final_sales[result$PHA.ID %in% all.hosp])/sum(rawdata$Sales)


write.xlsx(result[result$PHA.ID %in% all.hosp,], 'E:/MAX/Tide/MODEL/凯纷/051RF/2分之1_rf.xlsx')



####输出重要性的趋势图
pd2=function(data,var,l=200){
  
  if(is.numeric(data[,names(data)==var])){
    xmin=min(data[,names(data)==var])
    xmax=max(data[,names(data)==var])
    xseq=seq(xmin,xmax,len=l)
  }
  
  if(is.factor(data[,names(data)==var])){
    xseq = unique(data[,names(data)==var])
  }
  
  ma1=matrix(-1,length(xseq),2)
  ma1=data.frame(ma1)
  names(ma1)=c(var,'patrial_dependence')
  
  for(j in 1:length(xseq)){
    tmp1=data
    tmp1[,names(tmp1)==var] = xseq[j]
    tmp1$pred = f2(predict(A,tmp1))
    ma1[j,2]=mean(tmp1$pred)
    
    if(is.numeric(data[,names(data)==var])){
      ma1[j,1]=xseq[j]
    }
    if(is.factor(data[,names(data)==var])){
      ma1[j,1]=as.character(xseq)[j]
      if(ma1[j,1]=="0"){ma1[j,1]="其他"}
    }
    
  }
  
  return(ma1)
}


inds8 <- row.names(chk)[1:10]
for(j in 1:length(inds8)) {
  ma01 = pd2(data.frame(trn), inds8[j])
  jpeg(file=paste0('J:/Ipsen/MODEL/Etiasa/051RF/','Etiasa', inds8[j], ".jpeg"))
  par(mfrow=c(1,1))
  if(class(data.frame(trn)[,inds8[j]]) %in% c('numeric', 'integer')){
    plot(ma01,type='l')
    dev.off()
  }else{
    barplot(ma01[,2]
            ,xlab = names(ma01)[1]
            ,names.arg = ma01[,1],las=2,cex.names = 0.6)
    dev.off()
  }
}






####
universe <- read.xlsx('D:/Pharbers文件/Pfizer/Segments/Other MKT Segment_Pfizer_0823.xlsx')
xixi <- left_join(result, universe[,c('PHA.ID', 'Province', 'Prefecture')], by = c('PHA.ID'))
sum(is.na(xixi$Province))
xixi1 <- group_by(xixi, Province) %>% summarise(sales = sum(final_sales, na.rm = T))
