# Docker images
Some parts of the analysis are very resource intensive and will take several 
hours to run on an "average" desktop computer so the components of the analysis
have been divided into several docker containers in order to enable the use of
AWS's infrastructure. This repository has been tested on a Linux system
(Fedora 31).

Note that only the first two images are intended to be built and run locally,
 the others are intended to be used to define jobs in Amazon's BATCH dashboard
 and therefore are expected to be built locally but uploaded to Amazon's Elastic
 Container Registry (ECR). A typical command to upload an image to ECR is:

```
aws ecr get-login-password --region <aws-region> | docker login --username AWS --password-stdin <repository-ARN>
docker build -t <tag> .
docker tag <tag>:<version> <repository-ARN>:version
docker push <repository-ARN>:version
```
Also note that all the images that are meant to access AWS infrastructure, like
uploading or downloading objects from S3 buckets require a `secret_aws.txt` file
 with the appropriate access keys. All the folders have a "sample" of the file
and it is necessary to modify it to add the correct access keys.

The names of the folders are indicative of the purpose of the container defined
inside.

* **01-Base**: This is the image with the required environment to perform the
 analysis. It is based on the official CEntOS 8.2004 image and includes R with
 the required packages and their dependencies. This imagage should be created 
first because the others use it in their "FROM" clause as `uaqh/base`. It can 
be built and kept locally and the tag can be different as long as the other 
images are updated to refer to the new tag.  

	```
	# Build command
	docker build -t uaqh/base .
	```
* **02-Upload2S3**: This image runs R scripts that upload all data 
(in NetCDF format) to Amazon's S3. These scripts also apply the v01 corrections
 to the co-location data in order to estimate the uncertainties of the v01 
corrections. This image should be run locally and a credentials file
 (`secret_aws.txt`) is needed to make the aws.s3 library work (a template file
 is provided but it needs correct keys to be added, see 
[here for details](https://github.com/cloudyr/aws.s3)). Also, to run this image
it is necessary to provide the **absolute** path to the DATA and mount it to 
the /data folder in the image. See below command for an example. The structure
of the data folder is expected to be the same that is used in the Google Drive
folder.  

	```
	# Build command:  
	docker build -t uaqh/upload .
	  
	# Run command mounting the local `data` folder to the container:  
	docker run -it --rm -v "$PWD"/../../data/:/data uaqh/upload
	```
* **03-ReadNetCDF**: This image reads the NetCDF files from the S3 buckets and
 creates aggregated `RData` files for the subsequent analyses. This process 
takes a long time, depending on the requested time averaging. It takes a few
 minutes to generate the 1-hour average datasets but several hours to generate
 the 1-minute datasets. It is recommended that this image is run within AWS' 
infrastructure ([see AWS BATCH service](https://aws.amazon.com/batch/)) and 
use at least 32 cores and 120GB of memory to reduce these runtimes.

* **04-StatSummaries**: This image reads the `RData` files and constructs
 scatter and histogram plots for each device as well as constructing the linear
 models relating the inter-instrument uncertainty and other variables. The 
results are pushed to a new S3 bucket that includes a text summary for each 
device and the scatter and histogram plots. The models developed in this step 
are:
  * **PM2.5**: Uncertainties = a * PM2.5 + d
  * **Temperature**: Uncertainties = b * Temperature + d
  * **RH**: Uncertainties = c * RH + d
  * **FULL**: Uncertainties = a * PM2.5 + b * Temperature + c * RH + d
  * **LOG_ratio**: log((Uncertainties/PM2.5)^2) = b * log(Temperature) + d
* **05-Compare2colo**: This image takes the models and data from both 
co-locations and creates a consolidated dataset of model parameters for
 the **PM2.5** model that will be used by the uncertainties calculations. The
 resulting dataset is but on a new S3 bucket from where it will be used by the
 **AddUncertainties** image.
* **06-AddUncertainties**: Modify the NetCDF files for the deployments adding 
two new variables **err_pm2.5** and **err2_pm2.5** that correspond to the
 *inter-instrument* and *device-type* uncertainties respectively and places 
those files in a new set of S3 buckets retaining the original naming convention.
