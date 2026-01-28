import configparser
import os
import pytest

def setup():
    cfg = configparser.RawConfigParser()
    try:
        path = os.environ['S3VTESTS_CONF']
    except KeyError:
        raise RuntimeError(
            'To run tests, point environment '
            + 'variable s3VTESTS_CONF to a config file.',
            )
    cfg.read(path)

    if not cfg.defaults():
        raise RuntimeError('Your config file is missing the DEFAULT section!')
    if not cfg.has_section("s3 main"):
        raise RuntimeError('Your config file is missing the "s3 main" section!')

    defaults = cfg.defaults()

  	# vars from the DEFAULT section
    global default_host
    default_host = defaults.get("host")

    global default_port
    default_port = int(defaults.get("port"))
	# vars from the main section
    global main_access_key
    main_access_key = cfg.get('s3 main',"access_key")

    global main_secret_key
    main_secret_key = cfg.get('s3 main',"secret_key")

    # vars from the secondary section
    global secondary_host
    global secondary_port
    if cfg.has_section("secondary"):
        secondary_host = cfg.get('secondary',"host")
        secondary_port = int(cfg.get('secondary',"port"))
    else:
        secondary_host = None
        secondary_port = None


def get_config_host():
    global default_host
    return default_host


def get_config_port():
    global default_port
    return default_port


def get_access_key():
    global main_access_key
    return main_access_key


def get_secret_key():
    global main_secret_key
    return main_secret_key


def get_config_host2():
    global secondary_host
    return secondary_host


def get_config_port2():
    global secondary_port
    return secondary_port


@pytest.fixture(autouse=True, scope="package")
def configfile():
    setup()

