# AWS Java Client Example

## Introduction

This directory contains examples on how to use [AWS SDK for Java V1](https://github.com/aws/aws-sdk-java) to experience the RadosGW extensions with the S3 API.

## Feature

We support the following features in our example.
- Upload an object
- Create a bucket
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
# Clean and compile:
$ mvn clean compile assembly:single

# Run the example:
$ cd target/classes
$ java -cp ../AWSJavaClient-1.0-SNAPSHOT-jar-with-dependencies.jar AppendObject
```

If the above steps are all successful, you can see the result like this.
```
Successfully uploaded object myimage.jpg to bucket mybucket.
Listing objects in mybucket.
* myimage.jpg
```