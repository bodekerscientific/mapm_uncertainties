# Source the scripts in the right order
# ES642
source("./read_ES642_ncfiles_LOG_S3.R")
# Clear the workspace
rm(list=ls(all=TRUE))
# ODIN
source("./read_odin_ncfiles_LOG_S3.R")
# Clear the workspace
rm(list=ls(all=TRUE))
# For ERR2
source("./read_ncfiles_for_err2_S3.R")