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
bucket_in_all <- c("mapm-es642-raw-deployment",
                   "mapm-es642-v01-deployment",
                   "mapm-odin-raw-deployment",
                   "mapm-odin-v01-deployment")
models_in_all <- c("mapm-es642-raw-models",
                   "mapm-es642-v01-models",
                   "mapm-odin-raw-models",
                   "mapm-odin-v01-models")
bucket_in_err2 <- "err2-results"
bucket_out_all <- c("mapm-es642-raw-deployment-uncertainties",
                    "mapm-es642-v01-deployment-uncertainties",
                    "mapm-odin-raw-deployment-uncertainties",
                    "mapm-odin-v01-deployment-uncertainties")
bucket_out_shared <- "mapm-uncertainties-results-netcdf"
date1 <- as.POSIXct("2019-06-11 00:00", format = "%Y-%m-%d %H:%M")
date2 <- as.POSIXct("2019-09-01 00:00", format = "%Y-%m-%d %H:%M")

# Load data for err2
save_object(object = "allreadin2_comp.RData",
            bucket = bucket_in_err2,
            overwrite = TRUE,
            file = "allreadin2_comp.RData")
load("allreadin2_comp.RData")

# Calculate the instrument type error
# If the error is not determined (NAN on the expression), assume 0

tmp=subset(comp, (colo)=="colo1")
tmp2=subset(comp, (colo)=="colo2")

##colo-1
err2_odin_colo1 <- ifelse(!is.nan(exp(mean(tmp$odin_diff, na.rm=T))),
                          exp(mean(tmp$odin_diff, na.rm=T)),
                          0)
err2_es642_colo1 <- ifelse(!is.nan(exp(mean(tmp$es_diff, na.rm=T))),
                           exp(mean(tmp$es_diff, na.rm=T)),
                           0)
##colo-2
err2_odin_colo2 <- ifelse(!is.nan(exp(mean(tmp2$odin_diff, na.rm=T))),
                          exp(mean(tmp2$odin_diff, na.rm=T)),
                          0)
err2_es642_colo2 <- ifelse(!is.nan(exp(mean(tmp2$es_diff, na.rm=T))),
                           exp(mean(tmp2$es_diff, na.rm=T)),
                           0)

for (xi in (1:4)){
  bucket_in <- bucket_in_all[xi]
  models_in <- models_in_all[xi]
  bucket_out <- bucket_out_all[xi]
  
  if (!bucket_exists(bucket_out)[1]){
    put_bucket(bucket_out)
  }
  # Load models
  save_object(object = "all_params_1hour.RData",
              bucket = models_in,
              overwrite = TRUE,
              file = "all_params_1hour.RData")
  load("all_params_1hour.RData")
  # Other constants
  
  # Get the NetCDF filenames
  nc_files <- get_bucket_df(bucket_in)$Key
  nfiles <- length(nc_files)

  
  for (i in (1:nfiles)) {
    # Download object
    save_object(object = nc_files[i],
                bucket = bucket_in,
                overwrite = TRUE,
                file = nc_files[i])
    # Open file
    test_nc <- open.nc(paste0("./",nc_files[i]),write = TRUE)
    
    # Extract the time dimension
    time <- var.get.nc(test_nc,"time")
    time_posix <- as.POSIXct(time,origin = "2000-01-01")
    # Extract the PM2.5 variable
    pm2.5 <- var.get.nc(test_nc,"pm2.5")
    device <- strsplit(nc_files[i],"_Christ")[[1]][1]
    device_params <- all_params_1hour[which(all_params_1hour$deviceid==device),]
    
    #Build the uncertainties model
    # If both colocations are there, use them both
    # If only one, use that.
    # If the error is not determined (NAN on the expression), assume 0
    # First colocation
    u1_pm2.5 <- abs(device_params$int1 + pm2.5 * device_params$slp1) + device_params$sd1/2
    # Second colocation
    u2_pm2.5 <- abs(device_params$int2 + pm2.5 * device_params$slp2) + device_params$sd2/2
    
    u1_pm2.5 <- ifelse(!is.nan(u1_pm2.5),u1_pm2.5,0)
    u2_pm2.5 <- ifelse(!is.nan(u2_pm2.5),u2_pm2.5,0)
    
    # Add inter-instrument variability
    if (is.na(u1_pm2.5[1])){
      print("No first colo")
      print(device)
      u2_pm2.5 <- ifelse(!is.nan(u2_pm2.5),u2_pm2.5,0)
      u_pm2.5 <- u2_pm2.5
    } else if (is.na(u2_pm2.5[1])){
      print("No second colo")
      print(device)
      u_pm2.5 <- u1_pm2.5
    } else {
      u_pm2.5 <- as.numeric(time_posix - date1) * (u2_pm2.5 - u1_pm2.5)/as.numeric(date2 - date1) + u1_pm2.5
    }
    var.def.nc(test_nc,"pm2.5_uncertainty_1","NC_FLOAT",c("time"))
    att.put.nc(test_nc,"pm2.5_uncertainty_1","units","NC_CHAR","µg/m^3")
    att.put.nc(test_nc,"pm2.5_uncertainty_1","long_name","NC_CHAR","Intra-instrument variability uncertainty.")
    att.put.nc(test_nc,"pm2.5_uncertainty_1","standard_name","NC_CHAR","uncertainty_1_of_mass_concentration_of_pm2p5_ambient_aerosol_particles_in_air")
    att.put.nc(test_nc,"pm2.5_uncertainty_1","cell_methods","NC_CHAR","time: mean (interval: 1 minutes)")
    att.put.nc(test_nc,"pm2.5_uncertainty_1","missing_value","NC_FLOAT",-999.9)
    att.put.nc(test_nc,"pm2.5_uncertainty_1","description","NC_CHAR","One sided 1-sigma uncertainty (for every minute) resulting from intra-instrument variability, i.e. comparison between all instruments of the same instrument type.")
    var.put.nc(test_nc,"pm2.5_uncertainty_1",u_pm2.5)
    
    # Add instrument type error
    is_odin <- length(grep("ODIN",device)>0)
    is_es642 <- length(grep("ES-642",device)>0)
    err2 <- 0
    if (is_odin){
      err2 <- as.numeric(time_posix - date1) * (err2_odin_colo2 - err2_odin_colo1)/as.numeric(date2 - date1) + err2_odin_colo1
    } else if (is_es642){
      err2 <- as.numeric(time_posix - date1) * (err2_es642_colo2 - err2_es642_colo1)/as.numeric(date2 - date1) + err2_es642_colo1
    }
    var.def.nc(test_nc,"pm2.5_uncertainty_2","NC_FLOAT",c("time"))
    att.put.nc(test_nc,"pm2.5_uncertainty_2","units","NC_CHAR","µg/m^3")
    att.put.nc(test_nc,"pm2.5_uncertainty_2","long_name","NC_CHAR","Uncertainty associated with instrument type.")
    att.put.nc(test_nc,"pm2.5_uncertainty_2","standard_name","NC_CHAR","uncertainty_2_of_mass_concentration_of_pm2p5_ambient_aerosol_particles_in_air")
    att.put.nc(test_nc,"pm2.5_uncertainty_2","cell_methods","NC_CHAR","time: mean (interval: 1 minutes)")
    att.put.nc(test_nc,"pm2.5_uncertainty_2","missing_value","NC_FLOAT",-999.9)
    att.put.nc(test_nc,"pm2.5_uncertainty_2","description","NC_CHAR","One sided 1-sigma uncertainty resulting from comparison between all instruments and a reference instrument, here the TEOM.")
    var.put.nc(test_nc,"pm2.5_uncertainty_2",err2)

    # Add total uncertainty
    total_err <- err2 + u_pm2.5
    var.def.nc(test_nc,"pm2.5_uncertainty","NC_FLOAT",c("time"))
    att.put.nc(test_nc,"pm2.5_uncertainty","units","NC_CHAR","µg/m^3")
    att.put.nc(test_nc,"pm2.5_uncertainty","long_name","NC_CHAR","Total uncertainty of the measurements.")
    att.put.nc(test_nc,"pm2.5_uncertainty","standard_name","NC_CHAR","uncertainty_of_mass_concentration_of_pm2p5_ambient_aerosol_particles_in_air")
    att.put.nc(test_nc,"pm2.5_uncertainty","cell_methods","NC_CHAR","time: mean (interval: 1 minutes)")
    att.put.nc(test_nc,"pm2.5_uncertainty","missing_value","NC_FLOAT",-999.9)
    att.put.nc(test_nc,"pm2.5_uncertainty","description","NC_CHAR","One sided 1-sigma uncertainty resulting from adding intra-instrument and instrument type uncertainties.")
    var.put.nc(test_nc,"pm2.5_uncertainty",total_err)
    
    # Add General attributes
    att.put.nc(test_nc,"NC_GLOBAL","uncertainties","NC_CHAR","Calculated by Gustavo Olivares <Gustavo[.]Olivares.AT.niwa[.]co[.]nz> and Elizabeth Somervell <Elizabeth[.]Somervell.AT.niwa[.]co[.]nz>")

    close.nc(test_nc)
    # Build name with desired convention
    namelength <- nchar(nc_files[i])
    israw <- length(grep("raw",nc_files[i])>0)
    if (israw){
      object_name <- paste0(substr(nc_files[i],1,namelength-3),"_with_uncert.nc")
    } else {
      object_name <- paste0(substr(nc_files[i],1,namelength-3),"_1.nc")
    }
    put_object(file = nc_files[i],
               object = object_name,
               bucket = bucket_out,
               multipart = TRUE)
    put_object(file = nc_files[i],
               object = object_name,
               bucket = bucket_out_shared,
               multipart = TRUE)
    file.remove(nc_files[i])
  }
}