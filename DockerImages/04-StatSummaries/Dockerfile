# Base image fedora latest (centos:centos8.2.2004)
FROM uaqh/base:latest
# Remember to build uaqh/base

# Create the right folders
# copy files
COPY statistical_summaries_S3.R /statistical_summaries_S3.R
COPY secret_aws.txt /secret_aws.txt

# Run the R-packages
CMD Rscript statistical_summaries_S3.R
