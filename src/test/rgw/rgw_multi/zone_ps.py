import logging
import httplib
import urllib
import hmac
import hashlib
import base64
from time import gmtime, strftime
from multisite import Zone

log = logging.getLogger('rgw_multi.tests')


class PSZone(Zone):  # pylint: disable=too-many-ancestors
    """ PubSub zone class """
    def is_read_only(self):
        return True

    def tier_type(self):
        return "pubsub"

    def create(self, cluster, args=None, **kwargs):
        if args is None:
            args = ''
        args += ['--tier-type', self.tier_type()]
        return self.json_command(cluster, 'create', args)

    def has_buckets(self):
        return False


NO_HTTP_BODY = ''


def make_request(conn, method, resource, parameters=None):
    """generic request sending to pubsub radogw
    should cover: topics, notificatios and subscriptions
    """
    url_params = ''
    if parameters is not None:
        url_params = urllib.urlencode(parameters)
        # remove 'None' from keys with no values
        url_params = url_params.replace('=None', '')
        url_params = '?' + url_params
    string_date = strftime("%a, %d %b %Y %H:%M:%S +0000", gmtime())
    string_to_sign = method + '\n\n\n' + string_date + '\n' + resource
    signature = base64.b64encode(hmac.new(conn.aws_secret_access_key,
                                          string_to_sign.encode('utf-8'),
                                          hashlib.sha1).digest())
    headers = {'Authorization': 'AWS '+conn.aws_access_key_id+':'+signature,
               'Date': string_date,
               'Host': conn.host+':'+str(conn.port)}
    http_conn = httplib.HTTPConnection(conn.host, conn.port)
    if log.getEffectiveLevel() <= 10:
        http_conn.set_debuglevel(5)
    http_conn.request(method, resource+url_params, NO_HTTP_BODY, headers)
    response = http_conn.getresponse()
    data = response.read()
    status = response.status
    http_conn.close()
    return data, status


class PSTopic:
    """class to set/get/delete a topic
    PUT /topics/<topic name>
    GET /topics/<topic name>
    DELETE /topics/<topic name>
    """
    def __init__(self, conn, topic_name):
        self.conn = conn
        assert topic_name.strip()
        self.resource = '/topics/'+topic_name

    def send_request(self, method):
        """send request to radosgw"""
        return make_request(self.conn, method, self.resource)

    def get_config(self):
        """get topic info"""
        return self.send_request('GET')

    def set_config(self):
        """set topic"""
        return self.send_request('PUT')

    def del_config(self):
        """delete topic"""
        return self.send_request('DELETE')


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
        """setnotification"""
        return self.send_request('PUT', self.parameters)

    def del_config(self):
        """delete notification"""
        return self.send_request('DELETE', self.parameters)


class PSSubscription:
    """class to set/get/delete a subscription:
    PUT /subscriptions/<sub-name>?topic=<topic-name>
    GET /subscriptions/<sub-name>
    DELETE /subscriptions/<sub-name>
    also to get list of events, and ack them:
    GET /subscriptions/<sub-name>?events[&max-entries=<max-entries>][&marker=<marker>]
    POST /subscriptions/<sub-name>?ack&event-id=<event-id>
    """
    def __init__(self, conn, sub_name, topic_name):
        self.conn = conn
        assert topic_name.strip()
        self.resource = '/subscriptions/'+sub_name
        self.parameters = {'topic': topic_name}

    def send_request(self, method, parameters=None):
        """send request to radosgw"""
        return make_request(self.conn, method, self.resource, parameters)

    def get_config(self):
        """get subscription info"""
        return self.send_request('GET')

    def set_config(self):
        """set subscription"""
        return self.send_request('PUT', self.parameters)

    def del_config(self):
        """delete subscription"""
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
