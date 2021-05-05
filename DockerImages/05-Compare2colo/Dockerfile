# Base image fedora latest (centos:centos8.2.2004)
FROM uaqh/base:latest
# Remember to build uaqh/base

# Create the right folders
RUN mkdir -p /results
# copy files
COPY compare_pre_post_1hour_SS3.R /compare_pre_post_1hour_SS3.R
COPY secret_aws.txt /secret_aws.txt

# Run the R-packages
CMD Rscript compare_pre_post_1hour_SS3.R
