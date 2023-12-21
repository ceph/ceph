# Introduction
This directory contains Java code examples on how to upload an object to an S3 bucket running on a Ceph RGW cluster.

# Prerequisites
Linux machine running an RGW Ceph cluster. Preferably started with the ``OSD=1 MON=1 MDS=0 MGR=0 RGW=1 ../src/vstart.sh --debug --new `` command.  
Java and Maven installed on the Linux machine.  

## Workflow Procedure
1. Install AWS CLI version 1 on your Linux machine as explained [here](https://docs.aws.amazon.com/cli/v1/userguide/install-linux.html)
2. Create a bucket on the Ceph cluster with the command
``
aws --endpoint-url http://localhost:8000 s3 mb s3://sample-bucket
``
3. Navigate through your file system into the ``ceph-s3-upload`` folder  using your terminal. Please ensure you see the pom.xml file.
4. Run `` mvn clean package `` to install the required Java packages on the system.
5. Once successful, run `` java -jar target/ceph-s3-upload-1.0-SNAPSHOT-jar-with-dependencies.jar sample-bucket ceph-s3-upload.txt `` to test out Java s3 object upload on Ceph RGW cluster.
