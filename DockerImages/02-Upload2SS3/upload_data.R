# Source the scripts in the right order
# ES642
source("./ES642_data2s3.R")
# Clear the workspace
rm(list=ls(all=TRUE))
# ODIN
source("./ODIN_data2s3.R")
# Clear the workspace
rm(list=ls(all=TRUE))
# TEOM
source("./TEOM_data2s3.R")