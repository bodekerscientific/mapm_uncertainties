# Statistical summaries for ODIN data

# Libraries
library(readr)
library(ggplot2)
library(fitdistrplus)
library(openair)
library(parallel)
library(doParallel)
library(lubridate)
library(aws.s3)

aws_secrets <- read_delim("./secret_aws.txt",
                          delim = ";",
                          col_names = FALSE)

Sys.setenv("AWS_ACCESS_KEY_ID" = aws_secrets$X1,
           "AWS_SECRET_ACCESS_KEY" = aws_secrets$X2,
           "AWS_DEFAULT_REGION" = "ap-southeast-2")


# S3 buckets with NetCDF files
id_colo <- grep("colo",bucketlist()$Bucket)
id_results_colo <- id_colo[grep("results",bucketlist()$Bucket[id_colo])]
buckets_in <- bucketlist()$Bucket[id_results_colo]

# Other constants
# Time average
time_avgs <- c("1 hour","1 min")
t_strs <- c("1hour","1min")
data_in_files <- c("data_1hour.RData","data_1min.RData")
result_bucket <- "mapm-stat-summaries"

# For each bucket, handle the data
for (bucket_in in buckets_in){
  resultpath <- paste0("./results/",bucket_in,"/")
  is_odin <- (length(grep("odin",bucket_in)) > 0)
  for (tavID in (1:2)){
    time_avg <- time_avgs[tavID]
    t_str <- t_strs[tavID]
    data_in_file <- data_in_files[tavID]

    save_object(object = data_in_file,
                bucket = bucket_in,
                overwrite = TRUE,
                file = data_in_file)
    load(data_in_file)
    # Remove ZERO from PM2.5 channel ... they shouldn't be there!
    if (exists("output_data_1hour")){
      data_1hour <- subset(output_data_1hour,pm2.5>0)
      rm(output_data_1hour)
    } else {
      data_1hour <- subset(data_1hour,pm2.5>0)
    }
    
    # Create the (potentially) missing folders
    dir.create(paste0(resultpath,"scatter/",t_str), showWarnings = FALSE, recursive = TRUE)
    dir.create(paste0(resultpath,"histograms/",t_str), showWarnings = FALSE, recursive = TRUE)
    # Start "datewise"
    alldates <- unique(data_1hour$date)
    ndates <- length(alldates)
    
    # Uncertainties ... during co-locations they are certainties (bias)
    data_1hour$pm2.5_d <- (data_1hour$pm2.5 - data_1hour$pm2.5_fleet)
    
    # Now we need to describe the distributions of these "biases" 
    # and their relationships with different parameters
    
    # Devices
    if (is_odin){
      devices <- unique(data_1hour$odinID)
    } else {
      devices <- unique(data_1hour$ES642ID)
    }
    # Bucket for the fit models
    models <- list()
    # Histograms
    for (device in devices){
      if (is_odin){
        plot_data <- subset(data_1hour,odinID==device)
      } else {
        plot_data <- subset(data_1hour,ES642ID==device)
      }
      
      
      # Histogram
      device.histogram <- ggplot(data = plot_data, aes(x=pm2.5_d)) +
        geom_vline(aes(xintercept=mean(pm2.5_d,na.rm = TRUE)),
                  color="blue", linetype="dashed", size=1) +
        geom_vline(aes(xintercept=median(pm2.5_d,na.rm = TRUE)),
                  color="red", linetype="dashed", size=1) +
        geom_histogram(aes(y=..density..), colour="black", fill="white")+
        geom_density(alpha=.2, fill="#FF6666") +
        labs(title=device,x="PM2.5 deviation")
      ggsave(paste0(device,'.png'),
            plot = device.histogram,
            device = 'png',
            path = paste0(resultpath,"histograms/",t_str),
            width = 10,
            height = 10,
            units = 'cm')
      
      # Scatter plot
      device.scatter <- ggplot(data = plot_data, aes(x=air_temperature_fleet)) +
        geom_abline(aes(slope = 1, intercept = 0),
                    color="blue", linetype="dashed", size=1) +
        geom_point(aes(y = pm2.5_d), colour="red") +
        labs(title = device,
            x = "Fleet temperature",
            y = "Deviation from the consensus [%]")
      ggsave(paste0(device,'_temperature.png'),
            plot = device.scatter,
            device = 'png',
            path = paste0(resultpath,"scatter/",t_str),
            width = 10,
            height = 10,
            units = 'cm')
      device.scatter <- ggplot(data = plot_data, aes(x=pm2.5,y=pm2.5_d)) +
        geom_smooth(method=lm) +
        geom_point( colour="red") +
        labs(title = device,
            x = "PM2.5 [ug/m3]",
            y = "Deviation from the consensus [ug/m3]")
      
      ggsave(paste0(device,'_pm2.5.png'),
            plot = device.scatter,
            device = 'png',
            path = paste0(resultpath,"scatter/",t_str),
            width = 10,
            height = 10,
            units = 'cm')
      # Save the models
      models[[device]] <- list()
      
      # Numeric summaries to files by device
      capture.output({
        print(t_str)
        print(device)
        summary(models[[device]][['PM2.5']] <- lm(pm2.5_d~pm2.5,plot_data))
      },
      file = paste0(resultpath,device,".txt"),
      append = TRUE,
      type = "output")
      
      capture.output({
        print(t_str)
        print(device)
        summary(models[[device]][['Temperature']] <- lm(pm2.5_d~air_temperature,plot_data))
      },
      file = paste0(resultpath,device,".txt"),
      append = TRUE,
      type = "output")
      
      capture.output({
        summary(models[[device]][['RH']] <- lm(pm2.5_d~relative_humidity,plot_data))
      },
      file = paste0(resultpath,device,".txt"),
      append = TRUE,
      type = "output")
      
      capture.output({
        summary(models[[device]][['FULL']] <- lm(pm2.5_d~pm2.5 + air_temperature + relative_humidity,plot_data))
      },
      file = paste0(resultpath,device,".txt"),
      append = TRUE,
      type = "output")
      # Remove ZERO as it doesn't play nice with LOG
      idx_0 <- which(plot_data$pm2.5_d==0)
      plot_data[idx_0,14] <- NA
      capture.output({
        summary(models[[device]][['LOG_ratio']] <- lm(log((pm2.5_d/pm2.5)^2) ~ log(air_temperature),data = plot_data))
      },
      file = paste0(resultpath,device,".txt"),
      append = TRUE,
      type = "output")
    }
    
    model_list <- paste0('models.',t_str) 
    
    eval(parse(text=paste(model_list,"<- models")))
    
    save(list=model_list,file = paste0(resultpath,"models_",t_str,".RData"))
    put_object(file = paste0(resultpath,"models_",t_str,".RData"),
              object = paste0("models_",t_str,".RData"),
              bucket = bucket_in)
  }
  # Compress the plots and put them on a new bucket
  if (!bucket_exists(result_bucket)[1]){
    put_bucket(result_bucket)
  }
  system(paste("tar -zcvf",resultpath,"plots.tgz"))
  put_object(file = paste0("./plots.tgz"),
            object = paste0("stat_summaries.tgz"),
            bucket = result_bucket)
}

