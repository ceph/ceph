import configparser
import os
import pytest
from .api import admin

def setup():
    global cfg
    cfg = configparser.RawConfigParser()
    try:
        path = os.environ['BNTESTS_CONF']
    except KeyError:
        raise RuntimeError(
            'To run tests, point environment '
            + 'variable BNTESTS_CONF to a config file.',
            )
    cfg.read(path)

    if not cfg.defaults():
        raise RuntimeError('Your config file is missing the DEFAULT section!')
    if not cfg.has_section("s3 main"):
        raise RuntimeError('Your config file is missing the "s3 main" section!')
    defaults = cfg.defaults()

    global default_host
    default_host = defaults.get("host")

    global default_port
    default_port = int(defaults.get("port"))

    global default_zonegroup
    default_zonegroup = defaults.get("zonegroup")

    global default_cluster
    default_cluster = defaults.get("cluster")

    version = defaults.get("version")
    if version == "v1":
        _, result = admin(['zonegroup', 'modify', '--disable-feature=notification_v2'], default_cluster)
        if result != 0:
            raise RuntimeError('Failed to disable v2 notifications feature. error: '+str(result))
        _, result = admin(['period', 'update', '--commit'], default_cluster)
        if result != 0:
            raise RuntimeError('Failed to commit changes to period. error: '+str(result))
    elif version != "v2":
        raise RuntimeError('Invalid notification version: '+version)

    global main_access_key
    main_access_key = cfg.get('s3 main',"access_key")

    global main_secret_key
    main_secret_key = cfg.get('s3 main',"secret_key")

    global kerberos_service_name
    kerberos_service_name = cfg.get('kerberos', 'service_name', fallback=None)

    global kerberos_principal
    kerberos_principal = cfg.get('kerberos', 'principal', fallback=None)

    global kerberos_keytab
    kerberos_keytab = cfg.get('kerberos', 'keytab', fallback=None)

    global oauthbearer_token_endpoint_url
    oauthbearer_token_endpoint_url = cfg.get('oauthbearer', 'token_endpoint_url', fallback=None)

    global oauthbearer_client_id
    oauthbearer_client_id = cfg.get('oauthbearer', 'client_id', fallback=None)

    global oauthbearer_client_secret
    oauthbearer_client_secret = cfg.get('oauthbearer', 'client_secret', fallback=None)

    global oauthbearer_access_token
    oauthbearer_access_token = cfg.get('oauthbearer', 'access_token', fallback=None)

    global oauthbearer_scope
    oauthbearer_scope = cfg.get('oauthbearer', 'scope', fallback=None)

def get_config_host():
    global default_host
    return default_host

def get_config_port():
    global default_port
    return default_port

def get_config_zonegroup():
    global default_zonegroup
    return default_zonegroup

def get_config_cluster():
    global default_cluster
    return default_cluster

def get_access_key():
    global main_access_key
    return main_access_key

def get_secret_key():
    global main_secret_key
    return main_secret_key

def get_kerberos_config():
    global kerberos_service_name
    global kerberos_principal
    global kerberos_keytab
    return kerberos_service_name, kerberos_principal, kerberos_keytab

def get_oauthbearer_config():
    global oauthbearer_token_endpoint_url
    global oauthbearer_client_id
    global oauthbearer_client_secret
    global oauthbearer_access_token
    global oauthbearer_scope
    return (oauthbearer_token_endpoint_url, oauthbearer_client_id,
            oauthbearer_client_secret, oauthbearer_access_token,
            oauthbearer_scope)

@pytest.fixture(autouse=True, scope="package")
def configfile():
    setup()
