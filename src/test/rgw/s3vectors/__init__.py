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

    global default_zonegroup
    default_zonegroup = defaults.get("zonegroup")

    # name of the cluster, as used by "mrun". "noname" is a cluster started with
    # vstart.sh, while an mstart.sh cluster is named after its run directory
    global default_cluster
    default_cluster = defaults.get("cluster", "noname")

    # name of the RGW client, needed by "radosgw-admin" to pick a keyring when
    # the cluster is an installed one, and not a development one
    # when not set, the RGW is assumed to be started by "vstart", where its name
    # is based on the port it is listening on
    global default_rgw_client
    default_rgw_client = defaults.get("rgw_client")

    # the cluster holding the master zone. some "radosgw-admin" commands are
    # allowed only there. when not set, the tested cluster is used
    global master_cluster
    master_cluster = defaults.get("master_cluster")

    # vars from the main section
    global main_access_key
    main_access_key = cfg.get('s3 main',"access_key")

    global main_secret_key
    main_secret_key = cfg.get('s3 main',"secret_key")

    # vars from the secondary section
    global secondary_host
    global secondary_port
    global secondary_cluster
    if cfg.has_section("secondary"):
        secondary_host = cfg.get('secondary',"host")
        secondary_port = int(cfg.get('secondary',"port"))
        secondary_cluster = cfg.get('secondary',"cluster", fallback=default_cluster)
    else:
        secondary_host = None
        secondary_port = None
        secondary_cluster = None

    # s3vector backend type (local, s3, or rgw)
    global s3vector_backend
    s3vector_backend = defaults.get("s3vector_backend", "rgw")

    # backend options.
    # when not set, a value that fits the tested zone is used
    global s3vector_local_path
    s3vector_local_path = defaults.get("s3vector_local_path")

    global s3vector_s3_endpoint
    s3vector_s3_endpoint = defaults.get("s3vector_s3_endpoint")

    global s3vector_s3_region
    s3vector_s3_region = defaults.get("s3vector_s3_region")

    global s3vector_s3_allow_http
    s3vector_s3_allow_http = defaults.get("s3vector_s3_allow_http", "true")


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


def get_config_rgw_client():
    global default_rgw_client
    return default_rgw_client


def get_config_master_cluster():
    global master_cluster
    return master_cluster


def get_config_cluster2():
    global secondary_cluster
    return secondary_cluster


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


def get_s3vector_backend():
    global s3vector_backend
    return s3vector_backend


def get_s3vector_local_path():
    global s3vector_local_path
    return s3vector_local_path


def get_s3vector_s3_endpoint():
    global s3vector_s3_endpoint
    return s3vector_s3_endpoint


def get_s3vector_s3_region():
    global s3vector_s3_region
    return s3vector_s3_region


def get_s3vector_s3_allow_http():
    global s3vector_s3_allow_http
    return s3vector_s3_allow_http


def is_s3_backend():
    return get_s3vector_backend() in ("s3", "rgw")


@pytest.fixture(autouse=True, scope="package")
def configfile():
    setup()

