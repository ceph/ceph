import logging
import ssl
import urllib
import hmac
import hashlib
import base64
import xmltodict
from http import client as http_client
from urllib import parse as urlparse
from time import gmtime, strftime
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import os
import subprocess
import json

log = logging.getLogger('bucket_notification.tests')


# Boto3 compatibility wrapper classes to minimize code changes
class S3Key:
    """Wrapper class to provide boto-like interface for S3 objects using boto3"""
    def __init__(self, bucket, key_name, s3_client):
        self.bucket = bucket
        self.name = key_name
        self.key = key_name
        self._s3_client = s3_client
        self._metadata = {}
        self.etag = None
        self.version_id = None

    def set_contents_from_string(self, content):
        """Upload object content"""
        kwargs = {
            'Bucket': self.bucket.name,
            'Key': self.name,
            'Body': content
        }
        if self._metadata:
            kwargs['Metadata'] = self._metadata

        response = self._s3_client.put_object(**kwargs)
        self.etag = response['ETag']
        if 'VersionId' in response:
            self.version_id = response['VersionId']
        return response

    def set_metadata(self, key, value):
        """Set metadata for the object"""
        self._metadata[key] = value

    def delete(self):
        """Delete the object"""
        kwargs = {'Bucket': self.bucket.name, 'Key': self.name}
        if self.version_id:
            kwargs['VersionId'] = self.version_id
        return self._s3_client.delete_object(**kwargs)


class S3Bucket:
    """Wrapper class to provide boto-like interface for S3 buckets using boto3"""
    def __init__(self, connection, bucket_name):
        self.connection = connection
        self.name = bucket_name
        self._s3_client = connection._s3_client
        self._s3_resource = connection._s3_resource
        self._bucket = self._s3_resource.Bucket(bucket_name)

    def new_key(self, key_name):
        """Create a new key object"""
        return S3Key(self, key_name, self._s3_client)

    def list(self, prefix=''):
        """List objects in the bucket"""
        try:
            if prefix:
                objects = self._bucket.objects.filter(Prefix=prefix)
            else:
                objects = self._bucket.objects.all()

            # Convert to S3Key objects for compatibility
            keys = []
            for obj in objects:
                key = S3Key(self, obj.key, self._s3_client)
                key.etag = obj.e_tag
                if hasattr(obj, 'version_id'):
                    key.version_id = obj.version_id
                keys.append(key)
            return keys
        except Exception:
            # Return empty list if bucket is empty or error occurs
            return []

    def delete_key(self, key_name, version_id=None):
        """Delete a specific key"""
        kwargs = {'Bucket': self.name, 'Key': key_name}
        if version_id:
            kwargs['VersionId'] = version_id
        response = self._s3_client.delete_object(**kwargs)
        # Return a key-like object with version_id for compatibility
        deleted_key = S3Key(self, key_name, self._s3_client)
        if 'VersionId' in response:
            deleted_key.version_id = response['VersionId']
        if 'DeleteMarker' in response:
            deleted_key.delete_marker = response['DeleteMarker']
        return deleted_key

    def copy_key(self, new_key_name, src_bucket_name, src_key_name, src_version_id=None, metadata=None):
        """Copy a key within or between buckets"""
        copy_source = {'Bucket': src_bucket_name, 'Key': src_key_name}
        if src_version_id:
            copy_source['VersionId'] = src_version_id

        kwargs = {
            'CopySource': copy_source,
            'Bucket': self.name,
            'Key': new_key_name
        }

        if metadata is not None:
            kwargs['Metadata'] = metadata
            kwargs['MetadataDirective'] = 'REPLACE'

        response = self._s3_client.copy_object(**kwargs)

        # Return a key object for compatibility
        new_key = S3Key(self, new_key_name, self._s3_client)
        if 'CopyObjectResult' in response and 'ETag' in response['CopyObjectResult']:
            new_key.etag = response['CopyObjectResult']['ETag']
        if 'VersionId' in response:
            new_key.version_id = response['VersionId']
        return new_key

    def configure_versioning(self, enabled):
        """Enable or disable versioning on the bucket"""
        status = 'Enabled' if enabled else 'Suspended'
        self._s3_client.put_bucket_versioning(
            Bucket=self.name,
            VersioningConfiguration={'Status': status}
        )

    def initiate_multipart_upload(self, key_name, metadata=None):
        """Initiate a multipart upload"""
        kwargs = {'Bucket': self.name, 'Key': key_name}
        if metadata:
            kwargs['Metadata'] = metadata

        response = self._s3_client.create_multipart_upload(**kwargs)
        return MultipartUpload(self, key_name, response['UploadId'], self._s3_client)


class MultipartUpload:
    """Wrapper for multipart upload operations"""
    def __init__(self, bucket, key_name, upload_id, s3_client):
        self.bucket = bucket
        self.key_name = key_name
        self.upload_id = upload_id
        self._s3_client = s3_client
        self.parts = []

    def upload_part_from_file(self, fp, part_number, size=None):
        """Upload a part from a file object"""
        if size:
            data = fp.read(size)
        else:
            data = fp.read()

        response = self._s3_client.upload_part(
            Bucket=self.bucket.name,
            Key=self.key_name,
            PartNumber=part_number,
            UploadId=self.upload_id,
            Body=data
        )

        self.parts.append({
            'PartNumber': part_number,
            'ETag': response['ETag']
        })

    def complete_upload(self):
        """Complete the multipart upload"""
        # Sort parts by part number
        self.parts.sort(key=lambda x: x['PartNumber'])

        response = self._s3_client.complete_multipart_upload(
            Bucket=self.bucket.name,
            Key=self.key_name,
            UploadId=self.upload_id,
            MultipartUpload={'Parts': self.parts}
        )
        return response


class S3Connection:
    """Wrapper class to provide boto-like S3Connection interface using boto3"""
    def __init__(self, aws_access_key_id, aws_secret_access_key,
                 is_secure=True, port=None, host=None, calling_format=None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.access_key = aws_access_key_id  # Boto compatibility
        self.secret_key = aws_secret_access_key  # Boto compatibility
        self.is_secure = is_secure
        self.port = port
        self.host = host
        self.num_retries = 5  # Default for compatibility

        # Build endpoint URL
        protocol = 'https' if is_secure else 'http'
        self.endpoint_url = f'{protocol}://{host}:{port}'

        # Create boto3 client and resource
        # Use path-style addressing to match the old boto OrdinaryCallingFormat
        self._s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=Config(
                retries={'max_attempts': self.num_retries},
                s3={'addressing_style': 'path'},
                max_pool_connections=50
            )
        )

        self._s3_resource = boto3.resource(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            config=Config(
                s3={'addressing_style': 'path'},
                max_pool_connections=50
            )
        )

        # For SSL connections
        self.secure_conn = self if is_secure else None

    def create_bucket(self, bucket_name, **kwargs):
        """Create a bucket"""
        try:
            self._s3_client.create_bucket(Bucket=bucket_name)
        except ClientError as e:
            # Bucket might already exist
            if e.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                raise
        return S3Bucket(self, bucket_name)

    def get_bucket(self, bucket_name):
        """Get a bucket object"""
        return S3Bucket(self, bucket_name)

    def delete_bucket(self, bucket_name):
        """Delete a bucket"""
        return self._s3_client.delete_bucket(Bucket=bucket_name)



NO_HTTP_BODY = ''

def put_object_tagging(conn, bucket_name, key, tags):
    client = boto3.client('s3',
            endpoint_url='http://'+conn.host+':'+str(conn.port),
            aws_access_key_id=conn.aws_access_key_id,
            aws_secret_access_key=conn.aws_secret_access_key,
            config=Config(s3={'addressing_style': 'path'}))
    return client.put_object(Body='aaaaaaaaaaa', Bucket=bucket_name, Key=key, Tagging=tags)

def make_request(conn, method, resource, parameters=None, sign_parameters=False, extra_parameters=None):
    """generic request sending to pubsub radogw
    should cover: topics, notifications and subscriptions
    """
    url_params = ''
    if parameters is not None:
        url_params = urlparse.urlencode(parameters)
        # remove 'None' from keys with no values
        url_params = url_params.replace('=None', '')
        url_params = '?' + url_params
    if extra_parameters is not None:
        url_params = url_params + '&' + extra_parameters
    string_date = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
    string_to_sign = method + '\n\n\n' + string_date + '\n' + resource
    if sign_parameters:
        string_to_sign += url_params
    signature = base64.b64encode(hmac.new(conn.aws_secret_access_key.encode('utf-8'),
                                          string_to_sign.encode('utf-8'),
                                          hashlib.sha1).digest()).decode('ascii')
    headers = {'Authorization': 'AWS '+conn.aws_access_key_id+':'+signature,
               'Date': string_date,
               'Host': conn.host+':'+str(conn.port)}
    http_conn = http_client.HTTPConnection(conn.host, conn.port)
    if log.getEffectiveLevel() <= 10:
        http_conn.set_debuglevel(5)
    http_conn.request(method, resource+url_params, NO_HTTP_BODY, headers)
    response = http_conn.getresponse()
    data = response.read()
    status = response.status
    http_conn.close()
    return data.decode('utf-8'), status


def delete_all_objects(conn, bucket_name):
    client = boto3.client('s3',
                      endpoint_url='http://'+conn.host+':'+str(conn.port),
                      aws_access_key_id=conn.aws_access_key_id,
                      aws_secret_access_key=conn.aws_secret_access_key,
                      config=Config(s3={'addressing_style': 'path'}))

    objects = []
    for key in client.list_objects(Bucket=bucket_name)['Contents']:
        objects.append({'Key': key['Key']})
    # delete objects from the bucket
    response = client.delete_objects(Bucket=bucket_name,
            Delete={'Objects': objects})


class PSTopicS3:
    """class to set/list/get/delete a topic
    POST ?Action=CreateTopic&Name=<topic name>[&OpaqueData=<data>[&push-endpoint=<endpoint>&[<arg1>=<value1>...]]]
    POST ?Action=ListTopics
    POST ?Action=GetTopic&TopicArn=<topic-arn>
    POST ?Action=DeleteTopic&TopicArn=<topic-arn>
    """
    def __init__(self, conn, topic_name, region, endpoint_args=None, opaque_data=None, policy_text=None):
        self.conn = conn
        self.topic_name = topic_name.strip()
        assert self.topic_name
        self.topic_arn = ''
        self.attributes = {}
        if endpoint_args is not None:
            self.attributes = {nvp[0] : nvp[1] for nvp in urlparse.parse_qsl(endpoint_args, keep_blank_values=True)}
        if opaque_data is not None:
            self.attributes['OpaqueData'] = opaque_data
        if policy_text is not None:
            self.attributes['Policy'] = policy_text
        protocol = 'https' if conn.is_secure else 'http'
        self.client = boto3.client('sns',
                           endpoint_url=protocol+'://'+conn.host+':'+str(conn.port),
                           aws_access_key_id=conn.aws_access_key_id,
                           aws_secret_access_key=conn.aws_secret_access_key,
                           region_name=region,
                           verify='./cert.pem')

    def get_config(self, topic_arn=None):
        """get topic info"""
        parameters = {'Action': 'GetTopic', 'TopicArn': (topic_arn if topic_arn is not None else self.topic_arn)}
        body = urlparse.urlencode(parameters)
        string_date = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
        content_type = 'application/x-www-form-urlencoded; charset=utf-8'
        resource = '/'
        method = 'POST'
        string_to_sign = method + '\n\n' + content_type + '\n' + string_date + '\n' + resource
        log.debug('StringTosign: %s', string_to_sign)
        signature = base64.b64encode(hmac.new(self.conn.aws_secret_access_key.encode('utf-8'),
                                     string_to_sign.encode('utf-8'),
                                     hashlib.sha1).digest()).decode('ascii')
        headers = {'Authorization': 'AWS '+self.conn.aws_access_key_id+':'+signature,
                   'Date': string_date,
                   'Host': self.conn.host+':'+str(self.conn.port),
                   'Content-Type': content_type}
        if self.conn.is_secure:
            http_conn = http_client.HTTPSConnection(self.conn.host, self.conn.port,
                    context=ssl.create_default_context(cafile='./cert.pem'))
        else:
            http_conn = http_client.HTTPConnection(self.conn.host, self.conn.port)
        http_conn.request(method, resource, body, headers)
        response = http_conn.getresponse()
        data = response.read()
        status = response.status
        http_conn.close()
        dict_response = xmltodict.parse(data)
        return dict_response, status

    def set_config(self):
        """set topic"""
        result = self.client.create_topic(Name=self.topic_name, Attributes=self.attributes)
        self.topic_arn = result['TopicArn']
        return self.topic_arn
    
    def set_attributes(self, attribute_name, attribute_val, topic_arn=None):
        """set topic attributes."""
        result = self.client.set_topic_attributes(TopicArn=(
            topic_arn if topic_arn is not None else self.topic_arn), AttributeName=attribute_name, AttributeValue=attribute_val)
        return result['ResponseMetadata']['HTTPStatusCode']


    def del_config(self, topic_arn=None):
        """delete topic"""
        result = self.client.delete_topic(TopicArn=(topic_arn if topic_arn is not None else self.topic_arn))
        return result['ResponseMetadata']['HTTPStatusCode']

    def get_list(self):
        """list all topics"""
        # note that boto3 supports list_topics(), however, the result only show ARNs
        parameters = {'Action': 'ListTopics'}
        body = urlparse.urlencode(parameters)
        string_date = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
        content_type = 'application/x-www-form-urlencoded; charset=utf-8'
        resource = '/'
        method = 'POST'
        string_to_sign = method + '\n\n' + content_type + '\n' + string_date + '\n' + resource
        log.debug('StringTosign: %s', string_to_sign)
        signature = base64.b64encode(hmac.new(self.conn.aws_secret_access_key.encode('utf-8'),
                                     string_to_sign.encode('utf-8'),
                                     hashlib.sha1).digest()).decode('ascii')
        headers = {'Authorization': 'AWS '+self.conn.aws_access_key_id+':'+signature,
                   'Date': string_date,
                   'Host': self.conn.host+':'+str(self.conn.port),
                   'Content-Type': content_type}
        if self.conn.is_secure:
            http_conn = http_client.HTTPSConnection(self.conn.host, self.conn.port,
                    context=ssl.create_default_context(cafile='./cert.pem'))
        else:
            http_conn = http_client.HTTPConnection(self.conn.host, self.conn.port)
        http_conn.request(method, resource, body, headers)
        response = http_conn.getresponse()
        data = response.read()
        status = response.status
        http_conn.close()
        dict_response = xmltodict.parse(data)
        return dict_response, status

class PSNotificationS3:
    """class to set/get/delete an S3 notification
    PUT /<bucket>?notification
    GET /<bucket>?notification[=<notification>]
    DELETE /<bucket>?notification[=<notification>]
    """
    def __init__(self, conn, bucket_name, topic_conf_list):
        self.conn = conn
        assert bucket_name.strip()
        self.bucket_name = bucket_name
        self.resource = '/'+bucket_name
        self.topic_conf_list = topic_conf_list
        self.client = boto3.client('s3',
                                   endpoint_url='http://'+conn.host+':'+str(conn.port),
                                   aws_access_key_id=conn.aws_access_key_id,
                                   aws_secret_access_key=conn.aws_secret_access_key,
                                   config=Config(s3={'addressing_style': 'path'}))

    def send_request(self, method, parameters=None):
        """send request to radosgw"""
        return make_request(self.conn, method, self.resource,
                            parameters=parameters, sign_parameters=True)

    def get_config(self, notification=None):
        """get notification info"""
        parameters = None
        if notification is None:
            response = self.client.get_bucket_notification_configuration(Bucket=self.bucket_name)
            status = response['ResponseMetadata']['HTTPStatusCode']
            return response, status
        parameters = {'notification': notification}
        response, status = self.send_request('GET', parameters=parameters)
        dict_response = xmltodict.parse(response)
        return dict_response, status

    def set_config(self):
        """set notification"""
        response = self.client.put_bucket_notification_configuration(Bucket=self.bucket_name,
                                                                     NotificationConfiguration={
                                                                         'TopicConfigurations': self.topic_conf_list
                                                                     })
        status = response['ResponseMetadata']['HTTPStatusCode']
        return response, status

    def del_config(self, notification=None):
        """delete notification"""
        parameters = {'notification': notification}

        return self.send_request('DELETE', parameters)


test_path = os.path.normpath(os.path.dirname(os.path.realpath(__file__))) + '/../'

def bash(cmd, **kwargs):
    log.debug('running command: %s', ' '.join(cmd))
    kwargs['stdout'] = subprocess.PIPE
    process = subprocess.Popen(cmd, **kwargs)
    s = process.communicate()[0].decode('utf-8')
    return (s, process.returncode)

def admin(args, cluster='noname', **kwargs):
    """ radosgw-admin command """
    cmd = [test_path + 'test-rgw-call.sh', 'call_rgw_admin', cluster] + args
    return bash(cmd, **kwargs)

def ceph_admin(args, cluster='noname', **kwargs):
    """ ceph command """
    cmd = [test_path + 'test-rgw-call.sh', 'call_ceph', cluster] + args
    return bash(cmd, **kwargs)

def delete_all_topics(conn, tenant, cluster):
    """ delete all topics """
    if tenant == '':
        topics_result = admin(['topic', 'list'], cluster)
        topics_json = json.loads(topics_result[0])
        try:
            for topic in topics_json['topics']:
                admin(['topic', 'rm', '--topic', topic['name']], cluster)
        except TypeError:
            for topic in topics_json:
                admin(['topic', 'rm', '--topic', topic['name']], cluster)
    else:
        topics_result = admin(['topic', 'list', '--tenant', tenant], cluster)
        topics_json = json.loads(topics_result[0])
        try:
            for topic in topics_json['topics']:
                admin(['topic', 'rm', '--tenant', tenant, '--topic', topic['name']], cluster)
        except TypeError:
            for topic in topics_json:
                admin(['topic', 'rm', '--tenant', tenant, '--topic', topic['name']], cluster)

def set_rgw_config_option(client, option, value, cluster='noname'):
    """ change a config option """
    print(f'Setting {option} to {value} for {client} in cluster {cluster}')
    return ceph_admin(['config', 'set', client, option, str(value)], cluster)
