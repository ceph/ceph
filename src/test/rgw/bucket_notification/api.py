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
import os
import subprocess
import json

log = logging.getLogger('bucket_notification.tests')

NO_HTTP_BODY = ''

def put_object_tagging(conn, bucket_name, key, tags):
    client = boto3.client('s3',
            endpoint_url='http://'+conn.host+':'+str(conn.port),
            aws_access_key_id=conn.aws_access_key_id,
            aws_secret_access_key=conn.aws_secret_access_key)
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
                      aws_secret_access_key=conn.aws_secret_access_key)

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
                                   aws_secret_access_key=conn.aws_secret_access_key)

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

def delete_all_topics(conn, tenant, cluster):
    """ delete all topics """
    if tenant == '':
        topics_result = admin(['topic', 'list'], cluster)
        topics_json = json.loads(topics_result[0])
        for topic in topics_json['topics']:
            rm_result = admin(['topic', 'rm', '--topic', topic['name']], cluster)
            print(rm_result)
    else:
        topics_result = admin(['topic', 'list', '--tenant', tenant], cluster)
        topics_json = json.loads(topics_result[0])
        for topic in topics_json['topics']:
            rm_result = admin(['topic', 'rm', '--tenant', tenant, '--topic', topic['name']], cluster)
            print(rm_result)

