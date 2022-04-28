# AWS Golang Client Example

## Introduction

This directory contains examples on how to use [AWS SDK for Go V1](https://github.com/aws/aws-sdk-go) to experience the RadosGW extensions with the S3 API.

## Feature

We support the following features in our example.
- Upload an object
- List objects

## Quickstart

**Suppose you have already installed Ceph on your machine**; otherwise, you should follow the guidance about how to build and install Ceph. See more details [here](https://github.com/ceph/ceph#building-ceph).

Then, you could run the following command to run a local vstart cluster.

```
# Suppose you are under ceph/ directory now
$ cd build
$ env MON=1 OSD=1 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n -d
```

Finally, you could run the following commands to experience the example.

```
# Download dependencies (please ignore the warnings)
$ go mod download
$ go mod vendor

# Run the example to see how to upload the object myimage.jpg and list objects
$ go run append_object.go
```

If the above steps are all successful, you can see the result like this.
```
2022/04/29 02:38:13 Successfully uploaded object myimage.jpg to bucket mybucket.
Listing objects in mybucket:
2022/04/29 02:38:13 key = myimage.jpg
```