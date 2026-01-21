import logging
import boto3
from botocore.exceptions import ClientError

from itertools import zip_longest  # type: ignore

from nose.tools import eq_ as eq

from .multisite import *

from .conn import get_gateway_sts_connection

log = logging.getLogger(__name__)

def check_object_eq(k1, k2, check_extra = True):
    assert k1
    assert k2
    log.debug('comparing key name=%s', k1.key)
    eq(k1.key, k2.key)
    eq(k1.version_id, k2.version_id)
    eq(k1.is_latest, k2.is_latest)
    eq(k1.last_modified, k2.last_modified)

    # delete markers don't have 'size' attribute in boto3 resource API
    is_delete_marker_k1 = not hasattr(k1, 'size') or k1.size is None
    is_delete_marker_k2 = not hasattr(k2, 'size') or k2.size is None
    
    if is_delete_marker_k1:
        assert is_delete_marker_k2, "k1 is delete marker but k2 is not"
        return
    
    # regular objects
    obj1 = k1.Object()
    obj2 = k2.Object()
    
    # compare object contents
    body1 = obj1.get()['Body'].read()
    body2 = obj2.get()['Body'].read()
    eq(body1, body2)

    eq(obj1.metadata, obj2.metadata)
    eq(obj1.cache_control, obj2.cache_control)
    eq(obj1.content_type, obj2.content_type)
    eq(obj1.content_encoding, obj2.content_encoding)
    eq(obj1.content_disposition, obj2.content_disposition)
    eq(obj1.content_language, obj2.content_language)
    eq(obj1.e_tag, obj2.e_tag)

    if check_extra:
        eq(k1.owner['ID'], k2.owner['ID'])
        eq(k1.owner['DisplayName'], k2.owner['DisplayName'])

    eq(k1.storage_class, k2.storage_class)
    eq(k1.size, k2.size)
    encrypted1 = obj1.server_side_encryption is not None
    encrypted2 = obj2.server_side_encryption is not None
    eq(encrypted1, encrypted2)

class RadosZone(Zone):
    def __init__(self, name, zonegroup = None, cluster = None, data = None, zone_id = None, gateways = None):
        super(RadosZone, self).__init__(name, zonegroup, cluster, data, zone_id, gateways)

    def  tier_type(self):
        return "rados"


    class Conn(ZoneConn):
        def __init__(self, zone, credentials):
            super(RadosZone.Conn, self).__init__(zone, credentials)

        def get_bucket(self, name):
            try:
                bucket = self.s3_resource.Bucket(name)
                bucket.load()
                return bucket
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    return None
                raise

        def create_bucket(self, name):
            try:
                bucket = self.s3_resource.create_bucket(Bucket=name)
                return bucket
            except ClientError as e:
                if e.response['Error']['Code'] == '409':
                    return self.s3_resource.Bucket(name)
                raise

        def delete_bucket(self, name):
            bucket = self.s3_resource.Bucket(name)
            return bucket.delete()

        def check_bucket_eq(self, zone_conn, bucket_name):
            log.info('comparing bucket=%s zones={%s, %s}', bucket_name, self.name, zone_conn.name)
            b1 = self.get_bucket(bucket_name)
            b2 = zone_conn.get_bucket(bucket_name)

            b1_versions = list(b1.object_versions.all())
            log.debug('bucket1 objects:')
            for o in b1_versions:
                log.debug('o=%s', o.key)

            b2_versions = list(b2.object_versions.all())
            log.debug('bucket2 objects:')
            for o in b2_versions:
                log.debug('o=%s', o.name)

            for k1, k2 in zip_longest(b1_versions, b2_versions):
                if k1 is None:
                    log.critical('key=%s is missing from zone=%s', k2.key, self.name)
                    assert False
                if k2 is None:
                    log.critical('key=%s is missing from zone=%s', k1.key, zone_conn.name)
                    assert False

                check_object_eq(k1, k2)

                # check if delete marker
                try:
                    _ = k1.size
                    is_delete_marker = False
                except AttributeError:
                    is_delete_marker = True

                if is_delete_marker:
                    assert b1.Object(k1.key).get() is None  # should raise exception
                    assert b2.Object(k2.key).get() is None
                else:
                    # now get the keys through a HEAD operation, verify that the available data is the same
                    k1_head = b1.Object(k1.key)
                    k1_head.version_id = k1.version_id
                    k2_head = b2.Object(k2.key)
                    k2_head.version_id = k2.version_id
                    k1_head.load()
                    k2_head.load()

                    if k1.version_id:
                        # compare the olh to make sure they agree about current version
                        k1_olh = b1.Object(k1.key)
                        k2_olh = b2.Object(k2.key)
                        try:
                            k1_olh.load()
                            k2_olh.load()
                            # if there's a delete marker, load() will fail
                            if k1_olh and k2_olh:
                                check_object_eq(k1_olh, k2_olh, False)
                        except ClientError:
                            pass  # delete marker is current

            log.info('success, bucket identical: bucket=%s zones={%s, %s}', bucket_name, self.name, zone_conn.name)

            return True

        def get_role(self, role_name):
            return self.iam_conn.get_role(RoleName=role_name)

        def check_role_eq(self, zone_conn, role_name):
            log.info('comparing role=%s zones={%s, %s}', role_name, self.name, zone_conn.name)
            r1 = self.get_role(role_name)
            r2 = zone_conn.get_role(role_name)

            assert r1
            assert r2
            log.debug('comparing role name=%s', r1['get_role_response']['get_role_result']['role']['role_name'])
            eq(r1['get_role_response']['get_role_result']['role']['role_name'], r2['get_role_response']['get_role_result']['role']['role_name'])
            eq(r1['get_role_response']['get_role_result']['role']['role_id'], r2['get_role_response']['get_role_result']['role']['role_id'])
            eq(r1['get_role_response']['get_role_result']['role']['path'], r2['get_role_response']['get_role_result']['role']['path'])
            eq(r1['get_role_response']['get_role_result']['role']['arn'], r2['get_role_response']['get_role_result']['role']['arn'])
            eq(r1['get_role_response']['get_role_result']['role']['max_session_duration'], r2['get_role_response']['get_role_result']['role']['max_session_duration'])
            eq(r1['get_role_response']['get_role_result']['role']['assume_role_policy_document'], r2['get_role_response']['get_role_result']['role']['assume_role_policy_document'])

            log.info('success, role identical: role=%s zones={%s, %s}', role_name, self.name, zone_conn.name)

            return True

        def create_role(self, path, rolename, policy_document, tag_list):
            if policy_document is None:
                policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":[\"arn:aws:iam:::user/testuser\"]},\"Action\":[\"sts:AssumeRole\"]}]}"
            return self.iam_conn.create_role(RoleName=rolename, AssumeRolePolicyDocument=policy_document, Path=path)

        def delete_role(self, role_name):
            return self.iam_conn.delete_role(RoleName=role_name)

        def has_role(self, role_name):
            try:
                self.get_role(role_name)
            except ClientError:
                return False
            return True

        def put_role_policy(self, rolename, policyname, policy_document):
            if policy_document is None:
                policy_document = "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Resource\":\"*\",\"Action\":\"s3:*\"}]}"
            return self.iam_conn.put_role_policy(RoleName=rolename, PolicyName=policyname, PolicyDocument=policy_document)

        def create_topic(self, topicname, attributes):
            result = self.sns_client.create_topic(Name=topicname, Attributes=attributes)
            self.topic_arn = result['TopicArn']
            return self.topic_arn

        def delete_topic(self, topic_arn):
            return self.sns_client.delete_topic(TopicArn=topic_arn)

        def get_topic(self, topic_arn):
            return self.sns_client.get_topic_attributes(TopicArn=topic_arn)

        def list_topics(self):
            return self.sns_client.list_topics()['Topics']

        def create_notification(self, bucket_name, topic_conf_list):
            return self.s3_client.put_bucket_notification_configuration(
                Bucket=bucket_name, NotificationConfiguration={'TopicConfigurations': topic_conf_list})

        def delete_notifications(self, bucket_name):
            return self.s3_client.put_bucket_notification_configuration(Bucket=bucket_name,
                                                                        NotificationConfiguration={})

        def list_notifications(self, bucket_name):
            out = self.s3_client.get_bucket_notification_configuration(Bucket=bucket_name)
            if 'TopicConfigurations' in out:
              return out['TopicConfigurations']
            return []

        def head_object(self, bucket_name, obj_name):
            return self.s3_client.head_object(Bucket=bucket_name, Key=obj_name)

        def assume_role_create_bucket(self, bucket, role_arn, session_name, alt_user_creds):
            region = "" if self.zone.zonegroup is None else self.zone.zonegroup.name
            sts_conn = None
            if self.zone.gateways is not None:
                sts_conn = get_gateway_sts_connection(self.zone.gateways[0], alt_user_creds, region)
            assumed_role_object = sts_conn.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
            assumed_role_credentials = assumed_role_object['Credentials']
            credentials = Credentials(assumed_role_credentials['AccessKeyId'], assumed_role_credentials['SecretAccessKey'])
            self.get_temp_s3_connection(credentials, assumed_role_credentials['SessionToken'])
            self.temp_s3_client.create_bucket(Bucket=bucket)

    def get_conn(self, credentials):
        return self.Conn(self, credentials)

