##working with raw data but once you are happy must do it all again with version 1 data

library(RNetCDF)
library(readr)
library(aws.s3)
library(reshape2)

aws_secrets <- read_delim("./secret_aws.txt",
                          delim = ";",
                          col_names = FALSE)

Sys.setenv("AWS_ACCESS_KEY_ID" = aws_secrets$X1,
           "AWS_SECRET_ACCESS_KEY" = aws_secrets$X2,
           "AWS_DEFAULT_REGION" = "ap-southeast-2")
result_bucket <- "err2-results"
if (!bucket_exists(result_bucket)[1]){
  put_bucket(result_bucket)
}
##Load in 3 types of instrument data using Gus' code. Now using the second 'raw' data without linear interpolation.
print("#ODIN")
# Get all the file names from S3
bucketlist <- c('mapm-odin-nn-colo1',
                'mapm-odin-nn-colo2')
rm(nc_file_info)
for (bucket_in in bucketlist){
  if (!exists("nc_file_info")){
    nc_file_info <- data.frame(filename = as.character(get_bucket_df(bucket_in)$Key),
                               bucket = bucket_in,
                               stringsAsFactors = FALSE)
  } else {
    nc_file_info <- rbind(nc_file_info,data.frame(filename = get_bucket_df(bucket_in)$Key,
                                                  bucket = bucket_in,
                                                  stringsAsFactors = FALSE))
  }
}
# Only use the objects that are NC files
nc_file_info <- nc_file_info[grep("nc",nc_file_info$filename),]
nc_files <- nc_file_info$filename
nfiles <- length(nc_files)
for (i in (1:nfiles)){
  # Fetch object from S3
  save_object(object = nc_files[i],
              bucket = nc_file_info$bucket[i],
              overwrite = TRUE,
              file = nc_files[i])
  test_nc <- open.nc(paste0("./",nc_files[i]))
  all_data_list <- read.nc(test_nc)
  tmp_data <- as.data.frame(matrix(unlist(all_data_list[1:17]),nrow = length(all_data_list[[1]]),byrow=FALSE))
  names(tmp_data) <- names(all_data_list)[1:17]
  tmp_data$odinID <- substr(nc_files[i],1,11)
  if (i==1){
    all_data <- tmp_data
  } else {
    all_data <- rbind(all_data,tmp_data)
  }
  close.nc(test_nc)
  rm(test_nc)
  rm(all_data_list)
  rm(tmp_data)
  file.remove(nc_files[i])
}
all_data$date <- as.POSIXct(all_data$time,origin = "2000-01-01")

odin<-all_data
rm(all_data)

print("#ES642")
bucketlist <- c('mapm-es642-raw-colo1',
                'mapm-es642-raw-colo2')
rm(nc_file_info)
for (bucket_in in bucketlist){
  if (!exists("nc_file_info")){
    nc_file_info <- data.frame(filename = as.character(get_bucket_df(bucket_in)$Key),
                               bucket = bucket_in,
                               stringsAsFactors = FALSE)
  } else {
    nc_file_info <- rbind(nc_file_info,data.frame(filename = get_bucket_df(bucket_in)$Key,
                                                  bucket = bucket_in,
                                                  stringsAsFactors = FALSE))
  }
}
# Only use the objects that are NC files
nc_file_info <- nc_file_info[grep("nc",nc_file_info$filename),]
nc_files <- nc_file_info$filename
nfiles <- length(nc_files)

for (i in (1:nfiles)){
  print(i)
  # Fetch object from S3
  save_object(object = nc_files[i],
              bucket = nc_file_info$bucket[i],
              overwrite = TRUE,
              file = nc_files[i])
  test_nc <- open.nc(paste0("./",nc_files[i]))
  all_data_list <- read.nc(test_nc)
  n_names <- length(names(all_data_list))
  if (n_names > 13){
    n_cols <- 13
  } else {
    n_cols <- 11
  }
  tmp_data <- as.data.frame(matrix(unlist(all_data_list[1:n_cols]),nrow = length(all_data_list[[1]]),byrow=FALSE))
  names(tmp_data) <- names(all_data_list)[1:n_cols]
  tmp_data$date <- as.POSIXct(tmp_data$time,origin = "2000-01-01")  # Remove latitude, longitude and altitude
  tmp_data$es642ID <- substr(nc_files[i],1,11)
  if (i==1){
    all_data <- tmp_data
  } else {
    x <- try(rbind(all_data,tmp_data),silent = TRUE)
    if (class(x)!="try-error"){
      all_data <- rbind(all_data,tmp_data)
    }
  }
  close.nc(test_nc)
  rm(test_nc)
  rm(all_data_list)
  rm(tmp_data)
  file.remove(nc_files[i])
  
}
all_data$date <- as.POSIXct(all_data$time,origin = "2000-01-01")

es<-all_data
rm(all_data)

print("#TEOM")

bucketlist <- c('mapm-teom-raw-colo1',
                'mapm-teom-raw-colo2')
rm(nc_file_info)
for (bucket_in in bucketlist){
  if (!exists("nc_file_info")){
    nc_file_info <- data.frame(filename = as.character(get_bucket_df(bucket_in)$Key),
                               bucket = bucket_in,
                               stringsAsFactors = FALSE)
  } else {
    nc_file_info <- rbind(nc_file_info,data.frame(filename = get_bucket_df(bucket_in)$Key,
                                                  bucket = bucket_in,
                                                  stringsAsFactors = FALSE))
  }
}
# Only use the objects that are NC files
nc_file_info <- nc_file_info[grep("nc",nc_file_info$filename),]
nc_files <- nc_file_info$filename
nfiles <- length(nc_files)

for (i in (1:nfiles)){
  # Fetch object from S3
  save_object(object = nc_files[i],
              bucket = nc_file_info$bucket[i],
              overwrite = TRUE,
              file = nc_files[i])
  test_nc <- open.nc(paste0("./",nc_files[i]))
  all_data_list <- read.nc(test_nc)
  tmp_data <- as.data.frame(matrix(unlist(all_data_list[1:4]),nrow = length(all_data_list[[1]]),byrow=FALSE))
  names(tmp_data) <- names(all_data_list)[1:4]
  tmp_data$teomID <- substr(nc_files[i],1,11)
  if (i==1){
    all_data <- tmp_data
  } else {
    all_data <- rbind(all_data,tmp_data)
  }
  close.nc(test_nc)
  rm(test_nc)
  rm(all_data_list)
  rm(tmp_data)
  file.remove(nc_files[i])
  
}
all_data$date <- as.POSIXct(all_data$time,origin = "2000-01-01")

teom<-all_data
rm(all_data)

rm(i,nc_files,nfiles, datapath)

save.image("./allreadin2.RData")
put_object(file = "./allreadin2.RData",
           object = "allreadin2.RData",
           bucket = result_bucket)

##Check if data lognormally distributed
#odin PM2.5 - no - skewed to 0 - act as if lognormal for now hist(subset(odin$logpm2.5, odin$pm2.5>0))
#odin Temp - normal
#odin RH - no - skewed to 100
#es642 PM2.5 - lognormal
#es642 air temperature - normal
#es642 rh - normal - and low (after heating?)
#teom pm2.5 - lognormal (no temp & rh)

##Check minimumnumbers to see how to deal with zeros
odin$pm2.5_unzero=ifelse(odin$pm2.5>0,odin$pm2.5,1)
es$pm2.5_unzero=ifelse(es$pm2.5>0,es$pm2.5,0.1)
teom=subset(teom,(teomID)=="TEOM_Woolst")
teom$pm2.5_unzero=ifelse(teom$pm2.5>0,teom$pm2.5,1)
#log PM2.5
odin$logpm2.5=log(odin$pm2.5_unzero)
es$logpm2.5=log(es$pm2.5_unzero)
teom$logpm2.5=log(teom$pm2.5_unzero)

##Gus will be using 'raw' time period - 1 minute - so get fleet average first, then average up.
##for each minute find average and sd of 


###You need to take out the sites that are not in both colos
unique(subset(odin$odinID, odin$date<="2019-07-03 00:00:00 NZST"))
#"ODIN_SD0034"  "ODIN_SD0043" "ODIN_SD0044" "ODIN_SD0068" 
unique(subset(odin$odinID, odin$date>="2019-07-03 00:00:00 NZST"))
#"ODIN_SD0017"  

odin=subset(odin,!(odinID) %in% c("ODIN_SD0034","ODIN_SD0043","ODIN_SD0044","ODIN_SD0068","ODIN_SD0017"))

##Then make hourly average of mean of PM2.5
test=odin[,c(18,19,21)] ##21 pm2.5, ##8 temp ##9 RH
test2=reshape(test, idvar = "date", timevar = "odinID", direction = "wide") ##multiple rows match for odinID=ODIN_SD0022 & odinID=ODIN_SD0056: first taken
odin_wide=test2          
test=es[,c(14,15,17)] ##18 pm2.5, ##5 temp, ##6 RH
test2=reshape(test, idvar = "date", timevar = "es642ID", direction = "wide")##multiple rows match for es642ID 1,2,3,4,5,7
es_wide=test2
rm(test, test2)
###These are the DFs with minute resolution fleet averages
odin_wide$ave=apply(odin_wide[,c(2:46)],1,mean, na.rm=T)
odin_wide$stdev=apply(odin_wide[,c(2:46)],1,sd, na.rm=T)
es_wide$ave=apply(es_wide[,c(2:10)],1,mean, na.rm=T)
es_wide$stdev=apply(es_wide[,c(2:10)],1,sd, na.rm=T)
##Step 3
odin_hour=aggregate(odin_wide$ave, format(odin_wide["date"], "%Y-%m-%d-%H"), mean, na.rm=T)
odin_hour$date=as.POSIXct(strptime(odin_hour$date, format = "%Y-%m-%d-%H"))
es_hour=aggregate(es_wide$ave, format(es_wide["date"], "%Y-%m-%d-%H"), mean, na.rm=T)
es_hour$date=as.POSIXct(strptime(es_hour$date, format = "%Y-%m-%d-%H"))

rm(odin_wide,es_wide)

##Then make hourly average of mean of temp
test=odin[,c(18,19,8)] ##21 pm2.5, ##8 temp ##9 RH
test2=reshape(test, idvar = "date", timevar = "odinID", direction = "wide") ##multiple rows match for odinID=ODIN_SD0022 & odinID=ODIN_SD0056: first taken
odin_wide=test2          
test=es[,c(14,15,5)] ##18 pm2.5, ##5 temp, ##6 RH
test2=reshape(test, idvar = "date", timevar = "es642ID", direction = "wide")##multiple rows match for es642ID 1,2,3,4,5,7
es_wide=test2
rm(test, test2)
###These are the DFs with minute resolution fleet averages
odin_wide$ave=apply(odin_wide[,c(2:46)],1,mean, na.rm=T)
odin_wide$stdev=apply(odin_wide[,c(2:46)],1,sd, na.rm=T)
es_wide$ave=apply(es_wide[,c(2:10)],1,mean, na.rm=T)
es_wide$stdev=apply(es_wide[,c(2:10)],1,sd, na.rm=T)
##step 3
odin_hourt=aggregate(odin_wide$ave, format(odin_wide["date"], "%Y-%m-%d-%H"), mean, na.rm=T)
odin_hourt$date=as.POSIXct(strptime(odin_hourt$date, format = "%Y-%m-%d-%H"))
es_hourt=aggregate(es_wide$ave, format(es_wide["date"], "%Y-%m-%d-%H"), mean, na.rm=T)
es_hourt$date=as.POSIXct(strptime(es_hourt$date, format = "%Y-%m-%d-%H"))

rm(odin_wide,es_wide)

##Then make hourly average of mean of RH
test=odin[,c(18,19,9)] ##21 pm2.5, ##8 temp ##9 RH
test2=reshape(test, idvar = "date", timevar = "odinID", direction = "wide") ##multiple rows match for odinID=ODIN_SD0022 & odinID=ODIN_SD0056: first taken
odin_wide=test2          
test=es[,c(14,15,6)] ##18 pm2.5, ##5 temp, ##6 RH
test2=reshape(test, idvar = "date", timevar = "es642ID", direction = "wide")##multiple rows match for es642ID 1,2,3,4,5,7
es_wide=test2
rm(test, test2)
###These are the DFs with minute resolution fleet averages
odin_wide$ave=apply(odin_wide[,c(2:46)],1,mean, na.rm=T)
odin_wide$stdev=apply(odin_wide[,c(2:46)],1,sd, na.rm=T)
es_wide$ave=apply(es_wide[,c(2:10)],1,mean, na.rm=T)
es_wide$stdev=apply(es_wide[,c(2:10)],1,sd, na.rm=T)
##Step 3
odin_hourrh=aggregate(odin_wide$ave, format(odin_wide["date"], "%Y-%m-%d-%H"), mean, na.rm=T)
odin_hourrh$date=as.POSIXct(strptime(odin_hourrh$date, format = "%Y-%m-%d-%H"))
es_hourrh=aggregate(es_wide$ave, format(es_wide["date"], "%Y-%m-%d-%H"), mean, na.rm=T)
es_hourrh$date=as.POSIXct(strptime(es_hourrh$date, format = "%Y-%m-%d-%H"))

rm(odin_wide,es_wide)

##Merge all these with teom
comp=teom[,c(6,8)]
comp=merge(comp,odin_hour, by="date", all=T)
comp=merge(comp,odin_hourt, by="date", all=T)
comp=merge(comp,odin_hourrh, by="date", all=T)
comp=merge(comp,es_hour, by="date", all=T)
comp=merge(comp,es_hourt, by="date", all=T)
comp=merge(comp,es_hourrh, by="date", all=T)

names(comp)=c("date","logpm2.5","odin_pm2.5","odin_temp","odin_rh","es_pm2.5","es_temp","es_rh")
##Now need to distinguish which colo
comp$colo=ifelse(comp$date<="2019-07-03 00:00:00 NZST", "colo1","colo2")
comp$odin_diff=comp$odin_pm2.5-comp$logpm2.5
comp$es_diff=comp$es_pm2.5-comp$logpm2.5

save.image("./allreadin2_comp.RData")
write.csv(comp,"comp.csv", row.names=F)
put_object(file = "./allreadin2_comp.RData",
           object = "allreadin2_comp.RData",
           bucket = result_bucket)

put_object(file = "./comp.csv",
           object = "comp.csv",
           bucket = result_bucket)
