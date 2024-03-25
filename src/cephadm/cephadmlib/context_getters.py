# context_getters.py - extract configuration/values from context objects

import json
import sys
import os

from typing import Any, Dict, List, Optional, Tuple, Union

from .container_engines import Podman
from .context import CephadmContext
from .exceptions import Error
from .net_utils import EndPoint


cached_stdin = None


def _get_config_json(option: str) -> Dict[str, Any]:
    if not option:
        return dict()

    global cached_stdin
    if option == '-':
        if cached_stdin is not None:
            j = cached_stdin
        else:
            j = sys.stdin.read()
            cached_stdin = j
    else:
        # inline json string
        if option[0] == '{' and option[-1] == '}':
            j = option
        # json file
        elif os.path.exists(option):
            with open(option, 'r') as f:
                j = f.read()
        else:
            raise Error('Config file {} not found'.format(option))

    try:
        js = json.loads(j)
    except ValueError as e:
        raise Error('Invalid JSON in {}: {}'.format(option, e))
    else:
        return js


def get_parm(option: str) -> Dict[str, str]:
    js = _get_config_json(option)
    # custom_config_files is a special field that may be in the config
    # dict. It is used for mounting custom config files into daemon's containers
    # and should be accessed through the "fetch_custom_config_files" function.
    # For get_parm we need to discard it.
    js.pop('custom_config_files', None)
    return js


def fetch_meta(ctx: CephadmContext) -> Dict[str, Any]:
    """Return a dict containing metadata about a deployment."""
    meta = getattr(ctx, 'meta_properties', None)
    if meta is not None:
        return meta
    mjson = getattr(ctx, 'meta_json', None)
    if mjson is not None:
        meta = json.loads(mjson) or {}
        ctx.meta_properties = meta
        return meta
    return {}


def fetch_configs(ctx: CephadmContext) -> Dict[str, str]:
    """Return a dict containing arbitrary configuration parameters.
    This function filters out the key 'custom_config_files' which
    must not be part of a deployment's configuration key-value pairs.
    To access custom configuration file data, use `fetch_custom_config_files`.
    """
    # ctx.config_blobs is *always* a dict. it is created once when
    # a command is parsed/processed and stored "forever"
    cfg_blobs = getattr(ctx, 'config_blobs', None)
    if cfg_blobs:
        cfg_blobs = dict(cfg_blobs)
        cfg_blobs.pop('custom_config_files', None)
        return cfg_blobs
    # ctx.config_json is the legacy equivalent of config_blobs. it is a
    # string that either contains json or refers to a file name where
    # the file contains json.
    cfg_json = getattr(ctx, 'config_json', None)
    if cfg_json:
        jdata = _get_config_json(cfg_json) or {}
        jdata.pop('custom_config_files', None)
        return jdata
    return {}


def fetch_custom_config_files(ctx: CephadmContext) -> List[Dict[str, Any]]:
    """Return a list containing dicts that can be used to populate
    custom configuration files for containers.
    """
    # NOTE: this function works like the opposite of fetch_configs.
    # instead of filtering out custom_config_files, it returns only
    # the content in that key.
    cfg_blobs = getattr(ctx, 'config_blobs', None)
    if cfg_blobs:
        return cfg_blobs.get('custom_config_files', [])
    cfg_json = getattr(ctx, 'config_json', None)
    if cfg_json:
        jdata = _get_config_json(cfg_json)
        return jdata.get('custom_config_files', [])
    return []


def fetch_endpoints(ctx: CephadmContext) -> List[EndPoint]:
    """Return a list of Endpoints, which have a port and ip attribute"""
    ports = getattr(ctx, 'tcp_ports', None)
    if ports is None:
        ports = []
    if isinstance(ports, str):
        ports = list(map(int, ports.split()))
    port_ips: Dict[str, str] = {}
    port_ips_attr: Union[str, Dict[str, str], None] = getattr(
        ctx, 'port_ips', None
    )
    if isinstance(port_ips_attr, str):
        port_ips = json.loads(port_ips_attr)
    elif port_ips_attr is not None:
        # if it's not None or a str, assume it's already the dict we want
        port_ips = port_ips_attr

    endpoints: List[EndPoint] = []
    for port in ports:
        if str(port) in port_ips:
            endpoints.append(EndPoint(port_ips[str(port)], port))
        else:
            endpoints.append(EndPoint('0.0.0.0', port))

    return endpoints


def get_config_and_keyring(ctx):
    # type: (CephadmContext) -> Tuple[Optional[str], Optional[str]]
    config = None
    keyring = None

    d = fetch_configs(ctx)
    if d:
        config = d.get('config')
        keyring = d.get('keyring')
        if config and keyring:
            return config, keyring

    if 'config' in ctx and ctx.config:
        try:
            with open(ctx.config, 'r') as f:
                config = f.read()
        except FileNotFoundError as e:
            raise Error(e)

    if 'key' in ctx and ctx.key:
        keyring = '[%s]\n\tkey = %s\n' % (ctx.name, ctx.key)
    elif 'keyring' in ctx and ctx.keyring:
        try:
            with open(ctx.keyring, 'r') as f:
                keyring = f.read()
        except FileNotFoundError as e:
            raise Error(e)

    return config, keyring


def read_configuration_source(ctx: CephadmContext) -> Dict[str, Any]:
    """Read a JSON configuration based on the `ctx.source` value."""
    source = '-'
    if 'source' in ctx and ctx.source:
        source = ctx.source
    if source == '-':
        config_data = json.load(sys.stdin)
    else:
        with open(source, 'rb') as fh:
            config_data = json.load(fh)
    return config_data


def should_log_to_journald(ctx: CephadmContext) -> bool:
    if ctx.log_to_journald is not None:
        return ctx.log_to_journald
    return (
        isinstance(ctx.container_engine, Podman)
        and ctx.container_engine.supports_split_cgroups
    )
