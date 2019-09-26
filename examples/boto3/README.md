# Introduction
This directory contains examples on how to use boto3 to execrsize thew RadosGW extensions to the S3 API supported by AWS.

# Users
For the standard boto3 client to support these extensions, the: ``service-2.sdk-extras.json`` file should be placed under: ``~/.aws/models/s3/2006-03-01/`` directory.
For more information see [here](https://github.com/boto/botocore/blob/develop/botocore/loaders.py#L33).

# Developers
Anyone developing an extension to the S3 API supported by AWS, please modify ``service-2.sdk-extras.json`` (all extensions should go into the same file), so that boto3 could be used to test the new API. 
In addition, python files with code samples should be added to this directory demonstrating use of the new API.
When testing you changes please:
- make sure that the modified file is in the boto3 path as explained above
- make sure that the standard S3 tests suit is not broken, even with the extensions files in the path

