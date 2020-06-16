import boto
import boto.s3.connection


def get_gateway_connection(gateway, credentials):
    """ connect to the given gateway """
    if gateway.connection is None:
        gateway.connection = boto.connect_s3(
                aws_access_key_id = credentials.access_key,
                aws_secret_access_key = credentials.secret,
                host = gateway.host,
                port = gateway.port,
                is_secure = False,
                calling_format = boto.s3.connection.OrdinaryCallingFormat())
    return gateway.connection

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
