# Introduction
This directory contains Golang code examples on how to upload an object to an S3 bucket running on a Ceph RGW cluster.

# Prerequisite
Linux machine running an RGW Ceph cluster. Preferrably started with the ``OSD=1 MON=1 MDS=0 MGR=0 RGW=1 ../src/vstart.sh --debug --new `` command.  
Go installed on the Linux machine.  

## Workflow Procedure
1. Install AWS CLI version one on your Linux machine as explained [here](https://docs.aws.amazon.com/cli/v1/userguide/install-linux.html)
2. Create a bucket on the Ceph cluster with the command 
```
aws --endpoint-url http://localhost:8000 s3 mb s3://sample-bucket
```
3. Navigate through your file system to where the Go example code is using your terminal.
4. Run ``` go mod tidy ``` to install the required Go packages on the system.
5. Run the Go program as ``` go run object-upload.go sample-bucket fortuna.txt ``` on the terminal window to test out object upload to Ceph RGW cluster.
