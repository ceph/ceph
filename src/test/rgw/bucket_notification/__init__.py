import configparser
import os

def setup():
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

    global main_access_key
    main_access_key = cfg.get('s3 main',"access_key")

    global main_secret_key
    main_secret_key = cfg.get('s3 main',"secret_key")

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
