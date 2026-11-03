import configparser
import os
import pytest

def setup():
    cfg = configparser.RawConfigParser()
    try:
        path = os.environ['DEDUPTESTS_CONF']
    except KeyError:
        raise RuntimeError('To run tests, point environment '
                           + 'variable DEDUPTESTS_CONF to a config file.',)
    cfg.read(path)

    if not cfg.defaults():
        raise RuntimeError('Your config file is missing the DEFAULT section!')
    if not cfg.has_section("s3 main"):
        raise RuntimeError('Your config file is missing the "s3 main" section!')

    defaults = cfg.defaults()

    # vars from the DEFAULT section
    global default_zonegroup
    default_zonegroup = defaults.get("zonegroup")

    global default_zone
    default_zone = defaults.get("zone")

    global default_cluster
    default_cluster = defaults.get("cluster")

    global default_host
    default_host = defaults.get("host")

    global default_port
    default_port = int(defaults.get("port"))

    global default_data_pool
    default_data_pool = defaults.get("data_pool")
    if not default_data_pool:
        zone = defaults.get("zone")
        if zone:
            default_data_pool = zone + ".rgw.buckets.data"
        else:
            raise RuntimeError(
                'Your config file must set "data_pool" or "zone" in the DEFAULT section!')

    # vars from the main section
    global main_access_key
    main_access_key = cfg.get('s3 main',"access_key")

    global main_secret_key
    main_secret_key = cfg.get('s3 main',"secret_key")


def get_config_zonegroup():
    global default_zonegroup
    return default_zonegroup

def get_config_zone():
    global default_zone
    return default_zone

def get_config_cluster():
    global default_cluster
    return default_cluster


def get_config_host():
    global default_host
    return default_host


def get_config_port():
    global default_port
    return default_port


def get_config_data_pool():
    global default_data_pool
    return default_data_pool


def get_access_key():
    global main_access_key
    return main_access_key


def get_secret_key():
    global main_secret_key
    return main_secret_key


@pytest.fixture(autouse=True, scope="package")
def configfile():
    setup()
    yield
    # Lazy import avoids circular import: test_dedup imports from this package.
    from . import test_dedup
    test_dedup.restore_default_compression_via_period()
    test_dedup.close_all_connections()

