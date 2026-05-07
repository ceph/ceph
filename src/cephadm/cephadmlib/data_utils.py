# data_utils.py - assorted data management functions

import datetime
import os
import re
import uuid
import yaml
import logging

from configparser import ConfigParser

from typing import Dict, Any, Optional, Iterable, List, Union

from .constants import DATEFMT, DEFAULT_REGISTRY
from .context import CephadmContext
from .exceptions import Error


logger = logging.getLogger()


def dict_get(
    d: Dict, key: str, default: Any = None, require: bool = False
) -> Any:
    """
    Helper function to get a key from a dictionary.
    :param d: The dictionary to process.
    :param key: The name of the key to get.
    :param default: The default value in case the key does not
        exist. Default is `None`.
    :param require: Set to `True` if the key is required. An
        exception will be raised if the key does not exist in
        the given dictionary.
    :return: Returns the value of the given key.
    :raises: :exc:`self.Error` if the given key does not exist
        and `require` is set to `True`.
    """
    if require and key not in d.keys():
        raise Error('{} missing from dict'.format(key))
    return d.get(key, default)  # type: ignore


def dict_get_join(d: Dict[str, Any], key: str) -> Any:
    """
    Helper function to get the value of a given key from a dictionary.
    `List` values will be converted to a string by joining them with a
    line break.
    :param d: The dictionary to process.
    :param key: The name of the key to get.
    :return: Returns the value of the given key. If it was a `list`, it
        will be joining with a line break.
    """
    value = d.get(key)
    if isinstance(value, list):
        value = '\n'.join(map(str, value))
    return value


def read_config(fn):
    # type: (Optional[str]) -> ConfigParser
    cp = ConfigParser()
    if fn:
        cp.read(fn)
    return cp


def try_convert_datetime(s):
    # type: (str) -> Optional[str]
    # This is super irritating because
    #  1) podman and docker use different formats
    #  2) python's strptime can't parse either one
    #
    # I've seen:
    #  docker 18.09.7:  2020-03-03T09:21:43.636153304Z
    #  podman 1.7.0:    2020-03-03T15:52:30.136257504-06:00
    #                   2020-03-03 15:52:30.136257504 -0600 CST
    # (In the podman case, there is a different string format for
    # 'inspect' and 'inspect --format {{.Created}}'!!)

    # In *all* cases, the 9 digit second precision is too much for
    # python's strptime.  Shorten it to 6 digits.
    p = re.compile(r'(\.[\d]{6})[\d]*')
    s = p.sub(r'\1', s)

    # replace trailing Z with -0000, since (on python 3.6.8) it won't parse
    if s and s[-1] == 'Z':
        s = s[:-1] + '-0000'

    # cut off the redundant 'CST' part that strptime can't parse, if
    # present.
    v = s.split(' ')
    s = ' '.join(v[0:3])

    # try parsing with several format strings
    fmts = [
        '%Y-%m-%dT%H:%M:%S.%f%z',
        '%Y-%m-%d %H:%M:%S.%f %z',
    ]
    for f in fmts:
        try:
            # return timestamp normalized to UTC, rendered as DATEFMT.
            return (
                datetime.datetime.strptime(s, f)
                .astimezone(tz=datetime.timezone.utc)
                .strftime(DATEFMT)
            )
        except ValueError:
            pass
    return None


def is_fsid(s):
    # type: (str) -> bool
    try:
        uuid.UUID(s)
    except ValueError:
        return False
    return True


def normalize_image_digest(digest: str) -> str:
    """
    Normal case:
    >>> normalize_image_digest('ceph/ceph', 'quay.io')
    'quay.io/ceph/ceph'

    No change:
    >>> normalize_image_digest('quay.ceph.io/ceph/ceph', 'quay.io')
    'quay.ceph.io/ceph/ceph'

    >>> normalize_image_digest('quay.io/ubuntu', 'quay.io')
    'quay.io/ubuntu'

    >>> normalize_image_digest('localhost/ceph', 'quay.io')
    'localhost/ceph'
    """
    known_shortnames = [
        'ceph/ceph',
        'ceph/daemon',
        'ceph/daemon-base',
    ]
    for image in known_shortnames:
        if digest.startswith(image):
            return f'{DEFAULT_REGISTRY}/{digest}'
    return digest


def get_legacy_config_fsid(
    cluster: str, legacy_dir: Optional[str] = None
) -> Optional[str]:
    config_file = '/etc/ceph/%s.conf' % cluster
    if legacy_dir is not None:
        config_file = os.path.abspath(legacy_dir + config_file)

    if os.path.exists(config_file):
        config = read_config(config_file)
        if config.has_section('global') and config.has_option(
            'global', 'fsid'
        ):
            return config.get('global', 'fsid')
    return None


def get_legacy_daemon_fsid(
    ctx: CephadmContext,
    cluster: str,
    daemon_type: str,
    daemon_id: Union[int, str],
    legacy_dir: Optional[str] = None,
) -> Optional[str]:
    fsid = None
    if daemon_type == 'osd':
        try:
            fsid_file = os.path.join(
                ctx.data_dir, daemon_type, 'ceph-%s' % daemon_id, 'ceph_fsid'
            )
            if legacy_dir is not None:
                fsid_file = os.path.abspath(legacy_dir + fsid_file)
            with open(fsid_file, 'r') as f:
                fsid = f.read().strip()
        except IOError:
            pass
    if not fsid:
        fsid = get_legacy_config_fsid(cluster, legacy_dir=legacy_dir)
    return fsid


def _extract_host_info_from_applied_spec(
    f: Iterable[str],
) -> List[Dict[str, str]]:
    # overall goal of this function is to go through an applied spec and find
    # the hostname (and addr is provided) for each host spec in the applied spec.
    # Generally, we should be able to just pass the spec to the mgr module where
    # proper yaml parsing can happen, but for host specs in particular we want to
    # be able to distribute ssh keys, which requires finding the hostname (and addr
    # if possible) for each potential host spec in the applied spec.

    specs: List[str] = []
    current_spec: str = ''
    for line in f:
        if re.search(r'^---\s+', line):
            if current_spec:
                specs.append(current_spec)
            current_spec = ''
        else:
            if line:
                current_spec += line
    if current_spec:
        specs.append(current_spec)

    host_specs: List[Dict[str, Any]] = []
    for spec in specs:
        yaml_data = yaml.safe_load(spec)
        if 'service_type' in yaml_data.keys():
            if yaml_data['service_type'] == 'host':
                host_specs.append(yaml_data)
        else:
            spec_str = yaml.safe_dump(yaml_data)
            logger.error(
                f'Failed to pull service_type from spec:\n{spec_str}.'
            )

    host_dicts = []
    for s in host_specs:
        host_dict = _extract_host_info_from_spec(s)
        # if host_dict is empty here, we failed to pull the hostname
        # for the host from the spec. This should have already been logged
        # so at this point we just don't want to include it in our output
        if host_dict:
            host_dicts.append(host_dict)

    return host_dicts


def _extract_host_info_from_spec(host_spec: Dict[str, Any]) -> Dict[str, str]:
    # note:for our purposes here, we only really want the hostname
    # and address of the host from each of these specs in order to
    # be able to distribute ssh keys. We will later apply the spec
    # through the mgr module where proper yaml parsing can be done
    # The returned dicts from this function should only contain
    # one or two entries, one (required) for hostname, one (optional) for addr
    # {
    #   hostname: <hostname>
    #   addr: <ip-addr>
    # }
    # if we fail to find the hostname, an empty dict is returned

    host_dict = {}  # type: Dict[str, str]
    for field in ['hostname', 'addr']:
        try:
            host_dict[field] = host_spec[field]
        except KeyError as e:
            logger.error(
                f'Error trying to pull {field} from host spec:\n{host_spec}. Got error: {e}'
            )

    if 'hostname' not in host_dict:
        logger.error(f'Could not find hostname in host spec:\n{host_spec}')
        return {}
    return host_dict
