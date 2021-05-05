# Move data to an S3 bucket

library(readr)
library(aws.s3)
library(RNetCDF)
aws_secrets <- read_delim("./secret_aws.txt",
                          delim = ";",
                          col_names = FALSE)

Sys.setenv("AWS_ACCESS_KEY_ID" = aws_secrets$X1,
           "AWS_SECRET_ACCESS_KEY" = aws_secrets$X2,
           "AWS_DEFAULT_REGION" = "ap-southeast-2")

# ES642 RAW colocation 1 #######
file_in_path <- "./data/ES642/Colocation_1/Raw/NetCDF/"
nc_files <- dir(file_in_path,full.names = FALSE,pattern = "nc")

# Create bucket
bucket_name <- "mapm-es642-raw-colo1"
if (!bucket_exists(bucket_name)[1]){
  put_bucket(bucket_name)
}

for (nc_file in nc_files) {
  put_object(file = paste0(file_in_path,nc_file),
             object = nc_file,
             bucket = bucket_name)
}

# ES642 RAW colocation 2 #######
file_in_path <- "./data/ES642/Colocation_2/Raw/NetCDF/"
nc_files <- dir(file_in_path,full.names = FALSE,pattern = "nc")

# Create bucket
bucket_name <- "mapm-es642-raw-colo2"
if (!bucket_exists(bucket_name)[1]){
  put_bucket(bucket_name)
}
for (nc_file in nc_files) {
  put_object(file = paste0(file_in_path,nc_file),
             object = nc_file,
             bucket = bucket_name)
}

# ES642 v01 colocation 1 #######
file_in_path <- "./data/ES642/Colocation_1/Raw/NetCDF/"
file_out_path <- "./data/ES642/Colocation_1/v01/NetCDF/"
nc_files_in <- dir(file_in_path,full.names = FALSE,pattern = "nc")
nc_files <- stringr::str_replace(nc_files_in,"raw","v01")
file.copy(paste0(file_in_path,nc_files_in),
          paste0(file_out_path,nc_files),
          overwrite = TRUE)
v01.correction <- read_delim("./data/v01_ES642_coefficients.txt",
                             delim = "\t",
                             col_names = TRUE)
v01.correction <- v01.correction[,c(2,3,4)]
names(v01.correction) <- c("device","a","b")

# Create bucket
bucket_name <- "mapm-es642-v01-colo1"
if (!bucket_exists(bucket_name)[1]){
  put_bucket(bucket_name)
}
for (nc_file in nc_files) {
  # Get Device to match correction coefficients
  curr_dev <- paste0("ES642",substr(stringr::str_split(nc_file,
                                                       "_Christchurch")[[1]][1],
                                    8,
                                    50))
  id_dev <- which(v01.correction$device==curr_dev)
  if (length(id_dev)>0){
    if (!is.na(v01.correction$a[id_dev])){
      a <- 1
      b <- 0
    } else {
      a <- v01.correction$a[id_dev]
      b <- v01.correction$b[id_dev]
    }
    # Open NetCDF file
    curr_nc <- open.nc(paste0(file_out_path,nc_file),
                       write = TRUE)
    # Extract ONLY pm2.5 variable
    raw_pm2.5 <- var.get.nc(curr_nc,'pm2.5')
    v01_pm2.5 <- a * raw_pm2.5 + b
    var.put.nc(ncfile = curr_nc,
               variable = 'pm2.5',
               data = v01_pm2.5)
    sync.nc(curr_nc)
    close.nc(curr_nc)
  }
  put_object(file = paste0(file_out_path,nc_file),
             object = nc_file,
             bucket = bucket_name)
}

# ODIN v01 colocation 2 #######
file_in_path <- "./data/ES642/Colocation_1/Raw/NetCDF/"
file_out_path <- "./data/ES642/Colocation_1/v01/NetCDF/"
nc_files_in <- dir(file_in_path,full.names = FALSE,pattern = "nc")
nc_files <- stringr::str_replace(nc_files_in,"raw","v01")
file.copy(paste0(file_in_path,nc_files_in),
          paste0(file_out_path,nc_files),
          overwrite = TRUE)
v01.correction <- read_delim("./data/v01_ES642_coefficients.txt",
                             delim = "\t",
                             col_names = TRUE)
v01.correction <- v01.correction[,c(2,5,6)]
names(v01.correction) <- c("device","a","b")

# Create bucket
bucket_name <- "mapm-es642-v01-colo2"
if (!bucket_exists(bucket_name)[1]){
  put_bucket(bucket_name)
}
for (nc_file in nc_files) {
  # Get Device to match correction coefficients
  curr_dev <- paste0("ES642",substr(stringr::str_split(nc_file,
                                                       "_Christchurch")[[1]][1],
                                    8,
                                    50))
  id_dev <- which(v01.correction$device==curr_dev)
  if (length(id_dev)>0){
    if (!is.na(v01.correction$a[id_dev])){
      a <- 1
      b <- 0
    } else{
      a <- v01.correction$a[id_dev]
      b <- v01.correction$b[id_dev]
    }
    # Open NetCDF file
    curr_nc <- open.nc(paste0(file_out_path,nc_file),
                       write = TRUE)
    # Extract ONLY pm2.5 variable
    raw_pm2.5 <- var.get.nc(curr_nc,'pm2.5')
    v01_pm2.5 <- a * raw_pm2.5 + b
    var.put.nc(ncfile = curr_nc,
               variable = 'pm2.5',
               data = v01_pm2.5)
    sync.nc(curr_nc)
    close.nc(curr_nc)
  }
  put_object(file = paste0(file_out_path,nc_file),
             object = nc_file,
             bucket = bucket_name)
}


# ES642 RAW deployment #######
file_in_path <- "./data/ES642/Deployment/Raw/NetCDF/"
nc_files <- dir(file_in_path,full.names = FALSE,pattern = "nc")

# Create bucket
bucket_name <- "mapm-es642-raw-deployment"
if (!bucket_exists(bucket_name)[1]){
  put_bucket(bucket_name)
}
for (nc_file in nc_files) {
  put_object(file = paste0(file_in_path,nc_file),
             object = nc_file,
             bucket = bucket_name,
             multipart = TRUE)
}

# ES642 v01 deployment #######
file_in_path <- "./data/ES642/Deployment/v_01/NetCDF/"
nc_files <- dir(file_in_path,full.names = FALSE,pattern = "nc")

# Create bucket
bucket_name <- "mapm-es642-v01-deployment"
if (!bucket_exists(bucket_name)[1]){
  put_bucket(bucket_name)
}
for (nc_file in nc_files) {
  put_object(file = paste0(file_in_path,nc_file),
             object = nc_file,
             bucket = bucket_name,
             multipart = TRUE)
}

