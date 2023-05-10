# Introduction
This directory contains Golang code example on how to create an SNS Topic on a Ceph RGW cluster.

# Prerequisite
Linux machine running an RGW Ceph cluster. Preferrably started with the ``OSD=1 MON=1 MDS=0 MGR=0 RGW=1 ../src/vstart.sh --debug --new `` command.
Go installed on the Linux machine.

## Workflow Procedure
1. Navigate through your file system to where the Golang example code exists on your terminal.
2. Install the required Golang packages on the system.
```
go mod init examples/topic-creation/v2
go get github.com/aws/aws-sdk-go
go mod tidy
```
3. Run the Golang program as ``` go run topic-creation.go -t sample-topic-1 -a '{"push-endpoint": "http://127.0.0.1:10900"}' ``` on the terminal window to create SNS topic with custom attributes.
