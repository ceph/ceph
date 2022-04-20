# Introduction
This directory contains Jave code examples on how to upload an object to an S3 bucket running on a Ceph RadoGw cluster.

# Prerequisite
Linux machine running a RadoGw Ceph cluster having java and maven installed.

## Workflow Procedure
1. Install AWS CLI version one on your linux machine as explained [here](https://docs.aws.amazon.com/cli/v1/userguide/install-linux.html)
2. Once successful, run ```aws configure ``` on your terminal to enter the ceph cluster access key and secret key into the global shared configuration file.
3. Create a bucket on the Ceph cluster with the command 
```
aws --endpoint-url http://localhost:8000 s3 mb s3://sample-bucket
```
4. Navigate through your file system into the ceph-s3-upload folder  using your terminal. Please ensure you see the pom.xml file when you type ```ls``` in the terminal.
6. Run ``` mvn clean package ``` to install the required java packages on the system.
7. Once successful, run ``` java -jar target/ceph-s3-upload-1.0-SNAPSHOT-jar-with-dependencies.jar sample-bucket ceph-s3-upload.txt ``` to test out java s3 object upload on ceph Radogw cluster.

## Note
If the Ceph cluster is not running no localhost, please substitute the appropriate IP in the aws command and the java file
