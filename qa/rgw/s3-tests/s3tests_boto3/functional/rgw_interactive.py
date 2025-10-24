#!/usr/bin/python
import boto3
import os
import random
import string
import itertools

host = "localhost"
port = 8000

## AWS access key
access_key = "0555b35654ad1656d804"

## AWS secret key
secret_key = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=="

prefix = "YOURNAMEHERE-1234-"

endpoint_url = "http://%s:%d" % (host, port)

client = boto3.client(service_name='s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    endpoint_url=endpoint_url,
                    use_ssl=False,
                    verify=False)

s3 = boto3.resource('s3', 
                    use_ssl=False,
                    verify=False,
                    endpoint_url=endpoint_url, 
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key)

def choose_bucket_prefix(template, max_len=30):
    """
    Choose a prefix for our test buckets, so they're easy to identify.

    Use template and feed it more and more random filler, until it's
    as long as possible but still below max_len.
    """
    rand = ''.join(
        random.choice(string.ascii_lowercase + string.digits)
        for c in range(255)
        )

    while rand:
        s = template.format(random=rand)
        if len(s) <= max_len:
            return s
        rand = rand[:-1]

    raise RuntimeError(
        'Bucket prefix template is impossible to fulfill: {template!r}'.format(
            template=template,
            ),
        )

bucket_counter = itertools.count(1)

def get_new_bucket_name():
    """
    Get a bucket name that probably does not exist.

    We make every attempt to use a unique random prefix, so if a
    bucket by this name happens to exist, it's ok if tests give
    false negatives.
    """
    name = '{prefix}{num}'.format(
        prefix=prefix,
        num=next(bucket_counter),
        )
    return name

def get_new_bucket(session=boto3, name=None, headers=None):
    """
    Get a bucket that exists and is empty.

    Always recreates a bucket from scratch. This is useful to also
    reset ACLs and such.
    """
    s3 = session.resource('s3', 
                        use_ssl=False,
                        verify=False,
                        endpoint_url=endpoint_url, 
                        aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)
    if name is None:
        name = get_new_bucket_name()
    bucket = s3.Bucket(name)
    bucket_location = bucket.create()
    return bucket
