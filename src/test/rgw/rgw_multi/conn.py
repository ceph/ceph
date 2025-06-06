import boto
import boto.s3.connection
import boto.iam.connection
import boto.sts.connection
import boto3
from boto.regioninfo import RegionInfo

def get_gateway_connection(gateway, credentials):
    """ connect to the given gateway """
    # Always create a new connection to the gateway to ensure each set of credentials gets its own connection
    conn = boto.connect_s3(aws_access_key_id = credentials.access_key,
                           aws_secret_access_key = credentials.secret,
                           host = gateway.host,
                           port = gateway.port,
                           is_secure = False,
                           calling_format = boto.s3.connection.OrdinaryCallingFormat())
    if gateway.connection is None:
        gateway.connection = conn
    return conn

def get_gateway_secure_connection(gateway, credentials):
    """ secure connect to the given gateway """
    if gateway.ssl_port == 0:
        return None
    if gateway.secure_connection is None:
        gateway.secure_connection = boto.connect_s3(
            aws_access_key_id = credentials.access_key,
            aws_secret_access_key = credentials.secret,
            host = gateway.host,
            port = gateway.ssl_port,
            is_secure = True,
            validate_certs=False,
            calling_format = boto.s3.connection.OrdinaryCallingFormat())
    return gateway.secure_connection

def get_gateway_iam_connection(gateway, credentials, region):
    """ connect to iam api of the given gateway """
    if gateway.iam_connection is None:
        endpoint = f'http://{gateway.host}:{gateway.port}'
        print(endpoint)
        gateway.iam_connection = boto3.client(
                service_name = 'iam',
                aws_access_key_id = credentials.access_key,
                aws_secret_access_key = credentials.secret,
                endpoint_url = endpoint,
                region_name=region,
                use_ssl = False)
    return gateway.iam_connection

def get_gateway_sts_connection(gateway, credentials, region):
    """ connect to sts api of the given gateway """
    if gateway.sts_connection is None:
        endpoint = f'http://{gateway.host}:{gateway.port}'
        print(endpoint)
        gateway.sts_connection = boto3.client(
                service_name = 'sts',
                aws_access_key_id = credentials.access_key,
                aws_secret_access_key = credentials.secret,
                endpoint_url = endpoint,
                region_name=region,
                use_ssl = False)
    return gateway.sts_connection


def get_gateway_s3_client(gateway, credentials, region):
  """ connect to boto3 s3 client api of the given gateway """
  # Always create a new connection to the gateway to ensure each set of credentials gets its own connection
  s3_client = boto3.client('s3',
                           endpoint_url='http://' + gateway.host + ':' + str(gateway.port),
                           aws_access_key_id=credentials.access_key,
                           aws_secret_access_key=credentials.secret,
                           region_name=region)
  if gateway.s3_client is None:
      gateway.s3_client = s3_client
  return s3_client


def get_gateway_sns_client(gateway, credentials, region):
  """ connect to boto3 s3 client api of the given gateway """
  if gateway.sns_client is None:
      gateway.sns_client = boto3.client('sns',
                                        endpoint_url='http://' + gateway.host + ':' + str(gateway.port),
                                        aws_access_key_id=credentials.access_key,
                                        aws_secret_access_key=credentials.secret,
                                        region_name=region)
  return gateway.sns_client

def get_gateway_temp_s3_client(gateway, credentials, session_token, region):
  """ connect to boto3 s3 client api using temporary credntials """
  gateway.temp_s3_client = boto3.client('s3',
                        endpoint_url='http://' + gateway.host + ':' + str(gateway.port),
                        aws_access_key_id=credentials.access_key,
                        aws_secret_access_key=credentials.secret,
                        aws_session_token = session_token,
                        region_name=region)
  return gateway.temp_s3_client