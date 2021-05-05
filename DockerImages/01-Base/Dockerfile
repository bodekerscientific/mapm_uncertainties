# Base image fedora latest (centos:centos8.2.2004)
FROM centos:centos8.2.2004
# Remember to build uaqh/base

# Install dependencies
RUN dnf -y install epel-release \
    'dnf-command(config-manager)'
RUN dnf config-manager --set-enabled PowerTools
RUN dnf -y install \
    R \
    netcdf-devel \
    libjpeg-turbo-devel \
    openssl-devel \
    libxml2-devel \
    libcurl-devel

# Create the right folders
RUN mkdir -p /install
# copy files
COPY install_packages.R /install/install_packages.R

# install R-packages
RUN Rscript /install/install_packages.R
# Start BASH for testing and tidying
CMD ["/bin/bash"]
