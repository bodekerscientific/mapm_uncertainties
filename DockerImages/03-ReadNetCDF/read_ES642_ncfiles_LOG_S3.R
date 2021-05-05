# Read ES642 data from local NetCDF sources

# Libraries
library(readr)
library(RNetCDF)
library(openair)
library(parallel)
library(doParallel)
library(aws.s3)

aws_secrets <- read_delim("./secret_aws.txt",
                          delim = ";",
                          col_names = FALSE)

Sys.setenv("AWS_ACCESS_KEY_ID" = aws_secrets$X1,
           "AWS_SECRET_ACCESS_KEY" = aws_secrets$X2,
           "AWS_DEFAULT_REGION" = "ap-southeast-2")


# S3 buckets with NetCDF files
id_odin <- grep("mapm-es642",bucketlist()$Bucket)
id_colo <- id_odin[grep("colo",bucketlist()$Bucket[id_odin])]
id_results <- grep("results",bucketlist()$Bucket)
id_buckets_in <- id_colo[is.na(match(id_colo,id_results))]
buckets_in <- bucketlist()$Bucket[id_buckets_in]

# Other constants
# Time average
time_avgs <- c('1 hour','1 min')

for (bucket_in in buckets_in) {
  result_bucket <- paste0(bucket_in,"-results")
  if (!bucket_exists(result_bucket)[1]){
    put_bucket(result_bucket)
  }
  print(bucket_in)
  for (time_avg in time_avgs) {
    print(time_avg)
    df_name <- paste0('output_data_',gsub(' ','',time_avg))
    nc_files <- get_bucket_df(bucket_in)$Key
    nfiles <- length(nc_files)
    
    # We're paralellising this because it takes ages!
    cores <- detectCores()
    print("Read files")
    print(cores)
    cl <- makeCluster(cores) #not to overload your computer
    registerDoParallel(cl)
    
    output_data <- foreach(i=1:nfiles,
                         .packages=c("RNetCDF","openair","aws.s3"),
                         .combine=rbind,
                         .errorhandling = 'remove') %dopar%
      {
        save_object(object = nc_files[i],
                    bucket = bucket_in,
                    overwrite = TRUE,
                    file = nc_files[i])
        test_nc <- open.nc(paste0("./",nc_files[i]))
        all_data_list <- read.nc(test_nc)
        close.nc(test_nc)
        rm(test_nc)
        n_names <- length(names(all_data_list))
        if (n_names > 13){
          n_cols <- 13
        } else {
          n_cols <- 11
        }
        tmp_data <- as.data.frame(matrix(unlist(all_data_list[1:n_cols]),nrow = length(all_data_list[[1]]),byrow=FALSE))
        names(tmp_data) <- names(all_data_list)[1:n_cols]
        tmp_data$date <- as.POSIXct(tmp_data$time,origin = "2000-01-01")
        # Remove data with flagged pm2.5 data
        tmp_data <- subset(tmp_data,pm2.5_flag == 0)
        tmp_data <- timeAverage(tmp_data,avg.time = time_avg,start.date = as.POSIXct("2019-06-01 00:00:00"))
        tmp_data$ES642ID <- strsplit(nc_files[i],"_Christ")[[1]][1]
        tmp_data <- tmp_data[,c("date","pm2.5","pm2.5_flag","ES642ID","air_temperature","relative_humidity")]
        file.remove(nc_files[i])
        tmp_data
      }
    stopCluster(cl)
    # Deal with ZEROES in PM2.5
    z_idx <- which(output_data[,2]==0)
    if (length(z_idx)>0){
      output_data[z_idx,2] <- 1
    }
    # LOG transformations
    output_data$pm2.5_log <- log(output_data$pm2.5)
    
    # Fleet consensus
    # We're paralellising this because it takes ages!
    cores <- detectCores()
    print("Calculate means")
    print(cores)
    cl <- makeCluster(cores) #not to overload your computer
    registerDoParallel(cl)
    alldates <- unique(output_data$date)
    ndates <- length(alldates)
    
    output_data <- foreach(i=1:ndates,
                         .combine=rbind,
                         .errorhandling = 'remove') %dopar%
      {
        c_dates <- alldates[i]
        data_out <- subset(output_data, date == c_dates)
        c_pm1 <- mean(data_out$pm1,na.rm = TRUE)
        c_pm2.5 <- mean(data_out$pm2.5,na.rm = TRUE)
        c_pm10 <- mean(data_out$pm10,na.rm = TRUE)
        data_out$pm2.5_fleet <- exp(mean(data_out$pm2.5_log,na.rm = TRUE))
        data_out$pm2.5_fleet_sd <- exp(sd(data_out$pm2.5_log,na.rm = TRUE))
        data_out$pm2.5_fleet_n <- sum(!is.na(data_out$pm2.5_log))
        data_out$air_temperature_fleet <- mean(data_out$air_temperature,na.rm = TRUE)
        data_out$relative_humidity_fleet <- mean(data_out$relative_humidity,na.rm = TRUE)
        data_out
      }
    stopCluster(cl)
    output_data$pm2.5_fleet_se <- output_data$pm2.5_fleet_sd / sqrt(output_data$pm2.5_fleet_n)

    eval(parse(text=paste(df_name,"<- output_data")))
    out_file <- paste0("data_",gsub(' ','',time_avg),".RData")
    save(list=df_name,file = out_file)
    put_object(file = out_file,
               object = out_file,
               bucket = result_bucket)
  }
}


