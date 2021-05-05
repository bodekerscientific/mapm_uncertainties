# Compare first and second co-locations ES642

# Libraries
library(ggplot2)
library(aws.s3)
library(readr)

aws_secrets <- read_delim("./secret_aws.txt",
                          delim = ";",
                          col_names = FALSE)

Sys.setenv("AWS_ACCESS_KEY_ID" = aws_secrets$X1,
           "AWS_SECRET_ACCESS_KEY" = aws_secrets$X2,
           "AWS_DEFAULT_REGION" = "ap-southeast-2")

# Constants
t_str <- '1hour'
path_prefixes <- c("mapm-es642-raw-",
                   "mapm-es642-v01-",
                   "mapm-odin-nn-",
                   "mapm-odin-raw-",
                   "mapm-odin-v01-")
for (path_prefix in path_prefixes){
  is_odin <- (length(grep("odin",path_prefix)) > 0)
  colo1 <- paste0("results/",path_prefix,"colo1-results")
  colo2 <- paste0("results/",path_prefix,"colo2-results")
  bucket1 <- paste0(path_prefix,"colo1-results")
  bucket2 <- paste0(path_prefix,"colo2-results")
  
  # Load data
  save_object(object = "data_1hour.RData",
              bucket = bucket1,
              overwrite = TRUE,
              file = "data_1hour.RData")
  load("data_1hour.RData")
  if (exists("output_data_1hour")){
    data_1hour <- output_data_1hour
    rm(output_data_1hour)
  } else {
    data_1hour <- data_1hour
  }
  colo1.data <- data_1hour
  rm(data_1hour)
  file.remove("./data_1hour.RData")
  
  save_object(object = "data_1hour.RData",
              bucket = bucket2,
              overwrite = TRUE,
              file = "data_1hour.RData")
  load("data_1hour.RData")
  if (exists("output_data_1hour")){
    data_1hour <- output_data_1hour
    rm(output_data_1hour)
  } else {
    data_1hour <- data_1hour
  }
  colo2.data <- data_1hour
  rm(data_1hour)
  file.remove("./data_1hour.RData")
  
  # Load models
  
  save_object(object = "models_1hour.RData",
              bucket = bucket1,
              overwrite = TRUE,
              file = "models_1hour.RData")
  load("models_1hour.RData")
  colo1.models <- models.1hour
  rm(models.1hour)
  file.remove("./models_1hour.RData")
  
  save_object(object = "models_1hour.RData",
              bucket = bucket2,
              overwrite = TRUE,
              file = "models_1hour.RData")
  load("models_1hour.RData")
  colo2.models <- models.1hour
  rm(models.1hour)
  file.remove("./models_1hour.RData")
  
  # Per device
  if (is_odin){
    devices <- unique(c(colo1.data$odinID,colo2.data$odinID))
    # The final result
    all.params <- data.frame(deviceid = devices)
    all.params$int1<-NA
    all.params$slp1<-NA
    all.params$rsq1<-NA
    all.params$sd1<NA
    all.params$date1<-NA
    all.params$int2<-NA
    all.params$slp2<-NA
    all.params$rsq2<-NA
    all.params$sd2<NA
    all.params$date2<-NA
    
    i <- 1
    for (device in devices){
      print(device)
      mod1 <- colo1.models[[device]]
      mod2 <- colo2.models[[device]]
      med_date1 <- mean(subset(colo1.data,!is.na(pm2.5)&odinID==device)$date)
      med_date2 <- mean(subset(colo2.data,!is.na(pm2.5)&odinID==device)$date)
      sd1 <- sd(subset(colo1.data,odinID==device)$pm2.5,na.rm = TRUE)
      sd2 <- sd(subset(colo2.data,odinID==device)$pm2.5,na.rm = TRUE)
      if (length(mod1)>0){
        all.params$int1[i] <- mod1$PM2.5$coefficients[1]
        all.params$slp1[i] <- mod1$PM2.5$coefficients[2]
        all.params$rsq1[i] <- summary(mod1$PM2.5)$adj.r.squared
        all.params$sd1[i] <- sd1
        all.params$date1[i] <- med_date1
        
      }
      if (length(mod2)>0){
        all.params$int2[i] <- mod2$PM2.5$coefficients[1]
        all.params$slp2[i] <- mod2$PM2.5$coefficients[2]
        all.params$rsq2[i] <- summary(mod2$PM2.5)$adj.r.squared
        all.params$sd2[i] <- sd2
        all.params$date2[i] <- med_date2
      }
      i <- i + 1
    }
  } else {
    devices <- unique(c(colo1.data$ES642ID,colo2.data$ES642ID))
    # The final result
    all.params <- data.frame(deviceid = devices)
    all.params$int1<-NA
    all.params$slp1<-NA
    all.params$rsq1<-NA
    all.params$sd1<NA
    all.params$date1<-NA
    all.params$int2<-NA
    all.params$slp2<-NA
    all.params$rsq2<-NA
    all.params$sd2<NA
    all.params$date2<-NA
    
    i <- 1
    for (device in devices){
      print(device)
      mod1 <- colo1.models[[device]]
      mod2 <- colo2.models[[device]]
      med_date1 <- mean(subset(colo1.data,!is.na(pm2.5)&ES642ID==device)$date)
      med_date2 <- mean(subset(colo2.data,!is.na(pm2.5)&ES642ID==device)$date)
      sd1 <- sd(subset(colo1.data,ES642ID==device)$pm2.5,na.rm = TRUE)
      sd2 <- sd(subset(colo2.data,ES642ID==device)$pm2.5,na.rm = TRUE)
      
      if (length(mod1)>0){
        all.params$int1[i] <- mod1$PM2.5$coefficients[1]
        all.params$slp1[i] <- mod1$PM2.5$coefficients[2]
        all.params$rsq1[i] <- summary(mod1$PM2.5)$adj.r.squared
        all.params$sd1[i] <- sd1
        all.params$date1[i] <- med_date1
        
      }
      if (length(mod2)>0){
        all.params$int2[i] <- mod2$PM2.5$coefficients[1]
        all.params$slp2[i] <- mod2$PM2.5$coefficients[2]
        all.params$rsq2[i] <- summary(mod2$PM2.5)$adj.r.squared
        all.params$sd2[i] <- sd2
        all.params$date2[i] <- med_date2
      }
      i <- i + 1
    }
  }
  
  
  
  all_params_1hour <- all.params
  save(all_params_1hour,file = paste0("results/",path_prefix,"_all_params_1hour.RData"))
  result_bucket <- paste0(path_prefix,"models")
  if (!bucket_exists(result_bucket)[1]){
    put_bucket(result_bucket)
  }
  put_object(file = paste0("results/",path_prefix,"_all_params_1hour.RData"),
             object = "all_params_1hour.RData",
             bucket = result_bucket)
}