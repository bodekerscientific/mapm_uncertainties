# Base image fedora latest (centos:centos8.2.2004)
FROM uaqh/base:latest
# Remember to build uaqh/base

# Create the right folders
# copy files
COPY add_uncertainties_ncfiles_LOG_SS3.R /add_uncertainties_ncfiles_LOG_SS3.R
COPY secret_aws.txt /secret_aws.txt

# Run the R-packages
CMD Rscript add_uncertainties_ncfiles_LOG_SS3.R
