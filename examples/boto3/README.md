# Introduction
This directory contains examples on how to use AWS CLI/boto3 to exercise the RadosGW extensions to the S3 API.
This is an extension to the [AWS SDK](https://github.com/boto/botocore/blob/develop/botocore/data/s3/2006-03-01/service-2.json).

# Users
For the standard client to support these extensions, the: ``service-2.sdk-extras.json`` file should be placed under: ``~/.aws/models/s3/2006-03-01/`` directory.
For more information see [here](https://github.com/boto/botocore/blob/develop/botocore/loaders.py#L33).
## Python
The [boto3 client](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) could be used with the extensions, code samples exists in this directory.
## AWS CLI
The standard [AWS CLI](https://docs.aws.amazon.com/cli/latest/) may also be used with these extensions. For example:
- Unordered listing: 
```
aws --endpoint-url http://localhost:8000 s3api list-objects --bucket=mybucket --allow-unordered
```
- Bucket notifications with filtering extensions:
```
aws --region=default --endpoint-url http://localhost:8000 s3api put-bucket-notification-configuration --bucket mybucket --notification-configuration='{"TopicConfigurations": [{"Id": "notif1", "TopicArn": "arn:aws:sns:default::mytopic",  
"Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],  
"Filter": {"Metadata": {"FilterRules": [{"Name": "x-amz-meta-foo", "Value": "bar"}, {"Name": "x-amz-meta-hello", "Value": "world"}]}, "Key": {"FilterRules": [{"Name": "regex", "Value": "([a-z]+)"}]}}}]}'
 ```
- Get configuration of a specific notification of a bucket:
```
aws --endpoint-url http://localhost:8000 s3api get-bucket-notification-configuration --bucket=mybucket --notification=notif1
```

# Developers
Anyone developing an extension to the S3 API supported by AWS, please modify ``service-2.sdk-extras.json`` (all extensions should go into the same file), so that boto3 could be used to test the new API. 
In addition, python files with code samples should be added to this directory demonstrating use of the new API.
When testing you changes please:
- make sure that the modified file is in the boto3 path as explained above
- make sure that the standard S3 tests suit is not broken, even with the extensions files in the path

