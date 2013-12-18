#!/bin/bash 

wget -q http://ceph.com/qa/s3_bucket_quota.pls
wget -q http://ceph.com/qa/S3Lib.pm
perl s3_bucket_quota.pls 
exit 0

