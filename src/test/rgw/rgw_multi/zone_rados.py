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
    if hasattr(k1, 'version_id'):
        eq(k1.version_id, k2.version_id)
    if hasattr(k1, 'is_latest'):
        eq(k1.is_latest, k2.is_latest)
    if hasattr(k1, 'last_modified'):
        eq(k1.last_modified, k2.last_modified)

    response1 = k1.get()
    response2 = k2.get()
    
    body1 = response1['Body'].read()
    body2 = response2['Body'].read()
    eq(body1, body2)

    eq(response1.get('Metadata', {}), response2.get('Metadata', {}))
    eq(response1.get('CacheControl'), response2.get('CacheControl'))
    eq(response1.get('ContentType'), response2.get('ContentType'))
    eq(response1.get('ContentEncoding'), response2.get('ContentEncoding'))
    eq(response1.get('ContentDisposition'), response2.get('ContentDisposition'))
    eq(response1.get('ContentLanguage'), response2.get('ContentLanguage'))
    eq(response1.get('ETag'), response2.get('ETag'))

    if check_extra and hasattr(k1, 'owner') and hasattr(k2, 'owner'):
        eq(k1.owner.get('ID'), k2.owner.get('ID'))
        if 'DisplayName' in k1.owner and 'DisplayName' in k2.owner:
            eq(k1.owner['DisplayName'], k2.owner['DisplayName'])

    if hasattr(k1, 'storage_class'):
        eq(k1.storage_class, k2.storage_class)
    if hasattr(k1, 'size'):
        eq(k1.size, k2.size)

    encrypted1 = response1.get('ServerSideEncryption') is not None
    encrypted2 = response2.get('ServerSideEncryption') is not None
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
                self.s3_client.head_bucket(Bucket=name)
                # if no exception, bucket exists
                return self.s3_resource.Bucket(name)
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_codein ['404', 'NoSuchBucket']:
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
                log.debug('o=%s, v=%s', o.key, o.version_id)

            b2_versions = list(b2.object_versions.all())
            log.debug('bucket2 objects:')
            for o in b2_versions:
                log.debug('o=%s, v=%s', o.key, o.version_id)

            for k1, k2 in zip_longest(b1_versions, b2_versions):
                if k1 is None:
                    log.critical('key=%s is missing from zone=%s', k2.key, self.name)
                    assert False
                if k2 is None:
                    log.critical('key=%s is missing from zone=%s', k1.key, zone_conn.name)
                    assert False

                is_delete_marker_k1 = (not hasattr(k1, 'size') or k1.size is None)
                is_delete_marker_k2 = (not hasattr(k2, 'size') or k2.size is None)

                if is_delete_marker_k1 or is_delete_marker_k2:
                    # both must be delete markers
                    assert is_delete_marker_k1 and is_delete_marker_k2, \
                        f"delete marker mismatch: k1={is_delete_marker_k1}, k2={is_delete_marker_k2}"
                    eq(k1.key, k2.key)
                    eq(k1.version_id, k2.version_id)
                    if hasattr(k1, 'is_latest'):
                        eq(k1.is_latest, k2.is_latest)
                    log.debug('both are delete markers, skipping content comparison')
                    continue

                # regular objects - compare them
                check_object_eq(k1, k2)

                # additional verification
                k1_obj = b1.Object(k1.key)
                k2_obj = b2.Object(k2.key)
                k1_obj.load(VersionId=k1.version_id)
                k2_obj.load(VersionId=k2.version_id)

                if k1.version_id:
                    # compare OLH (current version marker)
                    k1_olh = b1.Object(k1.key)
                    k2_olh = b2.Object(k2.key)
                    try:
                        k1_olh.load()
                        k2_olh.load()
                        eq(k1_olh.e_tag, k2_olh.e_tag)
                        eq(k1_olh.content_length, k2_olh.content_length)
                    except ClientError:
                        pass  # current version is a delete marker

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

