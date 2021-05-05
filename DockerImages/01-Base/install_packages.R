# Install some really useful packages
# Set mirror
r <- getOption("repos")
r["CRAN"] <- "https://cloud.r-project.org" 
options(repos=r)
# Libraries
install.packages("readr")
install.packages("RNetCDF")
install.packages("openair")
install.packages("doParallel")
install.packages("aws.s3")
install.packages("stringr")
install.packages("reshape2")
install.packages("fitdistrplus")
install.packages("ggplot2")
install.packages("lubridate")

