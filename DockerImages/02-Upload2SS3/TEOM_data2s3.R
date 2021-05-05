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

# TEOM colocation 1 #######
file_in_path <- "./data/TEOM/Colocation_1/Raw/NetCDF/"
nc_files <- dir(file_in_path,full.names = FALSE,pattern = "nc")

# Create bucket
bucket_name <- "mapm-teom-raw-colo1"
if (!bucket_exists(bucket_name)[1]){
  put_bucket(bucket_name)
}
for (nc_file in nc_files) {
  put_object(file = paste0(file_in_path,nc_file),
             object = nc_file,
             bucket = bucket_name,
             multipart = TRUE)
}

# TEOM colocation 2 #######
file_in_path <- "./data/TEOM/Colocation_2/Raw/NetCDF/"
nc_files <- dir(file_in_path,full.names = FALSE,pattern = "nc")

# Create bucket
bucket_name <- "mapm-teom-raw-colo2"
if (!bucket_exists(bucket_name)[1]){
  put_bucket(bucket_name)
}
for (nc_file in nc_files) {
  put_object(file = paste0(file_in_path,nc_file),
             object = nc_file,
             bucket = bucket_name,
             multipart = TRUE)
}

# TEOM deployment #######
file_in_path <- "./data/TEOM/Deployment/Raw/NetCDF/"
nc_files <- dir(file_in_path,full.names = FALSE,pattern = "nc")

# Create bucket
bucket_name <- "mapm-teom-raw-deployment"
if (!bucket_exists(bucket_name)[1]){
  put_bucket(bucket_name)
}
for (nc_file in nc_files) {
  put_object(file = paste0(file_in_path,nc_file),
             object = nc_file,
             bucket = bucket_name,
             multipart = TRUE)
}


