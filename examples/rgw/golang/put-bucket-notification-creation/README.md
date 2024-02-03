# Introduction
This directory contains Golang code examples on how to create a put bucket notification to a topic and S3 bucket running on a Ceph RGW cluster.

# Prerequisite
Linux machine running an RGW Ceph cluster. Preferrably started with the ``OSD=1 MON=1 MDS=0 MGR=0 RGW=1 ../src/vstart.sh --debug --new `` command.
Go installed on the Linux machine.

## Workflow Procedure
1. Install AWS CLI version one on your Linux machine as explained [here](https://docs.aws.amazon.com/cli/v1/userguide/install-linux.html)
2. Create a topic on the Ceph cluster with the command
```
aws --region default --endpoint-url http://localhost:8000 sns create-topic --name=sample-topic --attributes='{"push-endpoint": "http://localhost:10900"}'
```
3. Create a bucket to which the topic will be attached to with the command
```
aws --endpoint-url http://localhost:8000 s3 mb s3://sample-bucket
```
4. Navigate through your file system to where the Golang example code exists on your terminal.
5. Install the required Golang packages on the system.
```
go mod init examples/put-bucket-notification-creation/v2
go get github.com/aws/aws-sdk-go
go mod tidy
```
6. Run the Golang program as ``` go run put-bucket-notification-creation.go -b sample-bucket -t arn:aws:sns:default::sample-topic ``` on the terminal window to create the put bucket notification with the suffix filter rule.
7. Upload  any jpg file you have to the bucket with the command
```
aws --endpoint-url http://localhost:8000 s3 cp your-jpg-file.jpg s3://sample-bucket
```
