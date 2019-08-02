import boto
import boto.s3.connection


def get_gateway_connection(gateway, credentials):
    """ connect to the given gateway """
    if gateway.connection is None:
        # client connections are initiated from a teuthology machine itself,
        # not one of the remotes - so we don't want to install certs there
        verify = False
        gateway.connection = boto.connect_s3(
                aws_access_key_id = credentials.access_key,
                aws_secret_access_key = credentials.secret,
                host = gateway.host,
                port = gateway.port,
                is_secure = gateway.proto == 'https',
                validate_certs = verify,
                calling_format = boto.s3.connection.OrdinaryCallingFormat())
    return gateway.connection

