# Base image fedora latest (centos:centos8.2.2004)
FROM uaqh/base:latest
# Remember to build uaqh/mapm-uncertainties

# Create the right folders
# copy files
COPY read_ES642_ncfiles_LOG_S3.R /read_ES642_ncfiles_LOG_S3.R
COPY read_odin_ncfiles_LOG_S3.R /read_odin_ncfiles_LOG_S3.R
COPY read_ncfiles_for_err2_S3.R /read_ncfiles_for_err2_S3.R
COPY read_ncfiles.R /read_ncfiles.R
COPY secret_aws.txt /secret_aws.txt

# Run the R-packages
CMD Rscript read_ncfiles.R
