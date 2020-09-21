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
from .multisite import Zone
import boto3
from botocore.client import Config

log = logging.getLogger('rgw_multi.tests')

def put_object_tagging(conn, bucket_name, key, tags):
    client = boto3.client('s3',
            endpoint_url='http://'+conn.host+':'+str(conn.port),
            aws_access_key_id=conn.aws_access_key_id,
            aws_secret_access_key=conn.aws_secret_access_key,
            config=Config(signature_version='s3'))
    return client.put_object(Body='aaaaaaaaaaa', Bucket=bucket_name, Key=key, Tagging=tags)


def get_object_tagging(conn, bucket, object_key):
    client = boto3.client('s3',
            endpoint_url='http://'+conn.host+':'+str(conn.port),
            aws_access_key_id=conn.aws_access_key_id,
            aws_secret_access_key=conn.aws_secret_access_key,
            config=Config(signature_version='s3'))
    return client.get_object_tagging(
                Bucket=bucket, 
                Key=object_key
            )


class PSZone(Zone):  # pylint: disable=too-many-ancestors
    """ PubSub zone class """
    def __init__(self, name, zonegroup=None, cluster=None, data=None, zone_id=None, gateways=None, full_sync='false', retention_days ='7'):
        self.full_sync = full_sync
        self.retention_days = retention_days
        self.master_zone = zonegroup.master_zone
        super(PSZone, self).__init__(name, zonegroup, cluster, data, zone_id, gateways)

    def is_read_only(self):
        return True

    def tier_type(self):
        return "pubsub"

    def syncs_from(self, zone_name):
        return zone_name == self.master_zone.name

    def create(self, cluster, args=None, **kwargs):
        if args is None:
            args = ''
        tier_config = ','.join(['start_with_full_sync=' + self.full_sync, 'event_retention_days=' + self.retention_days])
        args += ['--tier-type', self.tier_type(), '--sync-from-all=0', '--sync-from', self.master_zone.name, '--tier-config', tier_config] 
        return self.json_command(cluster, 'create', args)

    def has_buckets(self):
        return False


NO_HTTP_BODY = ''


def make_request(conn, method, resource, parameters=None, sign_parameters=False, extra_parameters=None):
    """generic request sending to pubsub radogw
    should cover: topics, notificatios and subscriptions
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


def print_connection_info(conn):
    """print info of connection"""
    print("Host: " + conn.host+':'+str(conn.port))
    print("AWS Secret Key: " + conn.aws_secret_access_key)
    print("AWS Access Key: " + conn.aws_access_key_id)


class PSTopic:
    """class to set/get/delete a topic
    PUT /topics/<topic name>[?push-endpoint=<endpoint>&[<arg1>=<value1>...]]
    GET /topics/<topic name>
    DELETE /topics/<topic name>
    """
    def __init__(self, conn, topic_name, endpoint=None, endpoint_args=None):
        self.conn = conn
        assert topic_name.strip()
        self.resource = '/topics/'+topic_name
        if endpoint is not None:
            self.parameters = {'push-endpoint': endpoint}
            self.extra_parameters = endpoint_args
        else:
            self.parameters = None
            self.extra_parameters = None

    def send_request(self, method, get_list=False, parameters=None, extra_parameters=None):
        """send request to radosgw"""
        if get_list:
            return make_request(self.conn, method, '/topics')
        return make_request(self.conn, method, self.resource, 
                            parameters=parameters, extra_parameters=extra_parameters)

    def get_config(self):
        """get topic info"""
        return self.send_request('GET')

    def set_config(self):
        """set topic"""
        return self.send_request('PUT', parameters=self.parameters, extra_parameters=self.extra_parameters)

    def del_config(self):
        """delete topic"""
        return self.send_request('DELETE')
    
    def get_list(self):
        """list all topics"""
        return self.send_request('GET', get_list=True)


def delete_all_s3_topics(zone, region):
    try:
        conn = zone.secure_conn if zone.secure_conn is not None else zone.conn
        protocol = 'https' if conn.is_secure else 'http'
        client = boto3.client('sns',
                endpoint_url=protocol+'://'+conn.host+':'+str(conn.port),
                aws_access_key_id=conn.aws_access_key_id,
                aws_secret_access_key=conn.aws_secret_access_key,
                region_name=region,
                verify='./cert.pem',
                config=Config(signature_version='s3'))

        topics = client.list_topics()['Topics']
        for topic in topics:
            print('topic cleanup, deleting: ' + topic['TopicArn'])
            assert client.delete_topic(TopicArn=topic['TopicArn'])['ResponseMetadata']['HTTPStatusCode'] == 200
    except Exception as err:
        print('failed to do topic cleanup: ' + str(err))
    

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
    return response


class PSTopicS3:
    """class to set/list/get/delete a topic
    POST ?Action=CreateTopic&Name=<topic name>[&OpaqueData=<data>[&push-endpoint=<endpoint>&[<arg1>=<value1>...]]]
    POST ?Action=ListTopics
    POST ?Action=GetTopic&TopicArn=<topic-arn>
    POST ?Action=DeleteTopic&TopicArn=<topic-arn>
    """
    def __init__(self, conn, topic_name, region, endpoint_args=None, opaque_data=None):
        self.conn = conn
        self.topic_name = topic_name.strip()
        assert self.topic_name
        self.topic_arn = ''
        self.attributes = {}
        if endpoint_args is not None:
            self.attributes = {nvp[0] : nvp[1] for nvp in urlparse.parse_qsl(endpoint_args, keep_blank_values=True)}
        if opaque_data is not None:
            self.attributes['OpaqueData'] = opaque_data
        protocol = 'https' if conn.is_secure else 'http'
        self.client = boto3.client('sns',
                           endpoint_url=protocol+'://'+conn.host+':'+str(conn.port),
                           aws_access_key_id=conn.aws_access_key_id,
                           aws_secret_access_key=conn.aws_secret_access_key,
                           region_name=region,
                           verify='./cert.pem',
                           config=Config(signature_version='s3'))


    def get_config(self):
        """get topic info"""
        parameters = {'Action': 'GetTopic', 'TopicArn': self.topic_arn}
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

    def del_config(self):
        """delete topic"""
        result = self.client.delete_topic(TopicArn=self.topic_arn)
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


class PSNotification:
    """class to set/get/delete a notification
    PUT /notifications/bucket/<bucket>?topic=<topic-name>[&events=<event>[,<event>]]
    GET /notifications/bucket/<bucket>
    DELETE /notifications/bucket/<bucket>?topic=<topic-name>
    """
    def __init__(self, conn, bucket_name, topic_name, events=''):
        self.conn = conn
        assert bucket_name.strip()
        assert topic_name.strip()
        self.resource = '/notifications/bucket/'+bucket_name
        if events.strip():
            self.parameters = {'topic': topic_name, 'events': events}
        else:
            self.parameters = {'topic': topic_name}

    def send_request(self, method, parameters=None):
        """send request to radosgw"""
        return make_request(self.conn, method, self.resource, parameters)

    def get_config(self):
        """get notification info"""
        return self.send_request('GET')

    def set_config(self):
        """set notification"""
        return self.send_request('PUT', self.parameters)

    def del_config(self):
        """delete notification"""
        return self.send_request('DELETE', self.parameters)


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
                                   config=Config(signature_version='s3'))

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


class PSSubscription:
    """class to set/get/delete a subscription:
    PUT /subscriptions/<sub-name>?topic=<topic-name>[&push-endpoint=<endpoint>&[<arg1>=<value1>...]]
    GET /subscriptions/<sub-name>
    DELETE /subscriptions/<sub-name>
    also to get list of events, and ack them:
    GET /subscriptions/<sub-name>?events[&max-entries=<max-entries>][&marker=<marker>]
    POST /subscriptions/<sub-name>?ack&event-id=<event-id>
    """
    def __init__(self, conn, sub_name, topic_name, endpoint=None, endpoint_args=None):
        self.conn = conn
        assert topic_name.strip()
        self.resource = '/subscriptions/'+sub_name
        if endpoint is not None:
            self.parameters = {'topic': topic_name, 'push-endpoint': endpoint}
            self.extra_parameters = endpoint_args
        else:
            self.parameters = {'topic': topic_name}
            self.extra_parameters = None

    def send_request(self, method, parameters=None, extra_parameters=None):
        """send request to radosgw"""
        return make_request(self.conn, method, self.resource, 
                            parameters=parameters,
                            extra_parameters=extra_parameters)

    def get_config(self):
        """get subscription info"""
        return self.send_request('GET')

    def set_config(self):
        """set subscription"""
        return self.send_request('PUT', parameters=self.parameters, extra_parameters=self.extra_parameters)

    def del_config(self, topic=False):
        """delete subscription"""
        if topic:
            return self.send_request('DELETE', self.parameters)
        return self.send_request('DELETE')

    def get_events(self, max_entries=None, marker=None):
        """ get events from subscription """
        parameters = {'events': None}
        if max_entries is not None:
            parameters['max-entries'] = max_entries
        if marker is not None:
            parameters['marker'] = marker
        return self.send_request('GET', parameters)

    def ack_events(self, event_id):
        """ ack events in a subscription """
        parameters = {'ack': None, 'event-id': event_id}
        return self.send_request('POST', parameters)


class PSZoneConfig:
    """ pubsub zone configuration """
    def __init__(self, cfg, section):
        self.full_sync = cfg.get(section, 'start_with_full_sync')
        self.retention_days = cfg.get(section, 'retention_days')
