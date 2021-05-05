# Base image fedora latest (centos:centos8.2.2004)
FROM uaqh/base:latest
# Remember to build uaqh/mapm-uncertainties

# Create the right folders
# copy files
COPY ES642_data2s3.R /ES642_data2s3.R
COPY ODIN_data2s3.R /ODIN_data2s3.R
COPY TEOM_data2s3.R /TEOM_data2s3.R
COPY upload_data.R /upload_data.R
COPY secret_aws.txt /secret_aws.txt

# Run the R-packages
CMD Rscript upload_data.R
