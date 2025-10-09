#!/bin/bash
set -ex
cpanm --sudo Amazon::S3
exec perl $(dirname $0)/s3_multipart_upload.pl
