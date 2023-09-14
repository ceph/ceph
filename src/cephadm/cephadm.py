#!/usr/bin/python3

import asyncio
import asyncio.subprocess
import argparse
import datetime
import fcntl
import ipaddress
import io
import json
import logging
from logging.config import dictConfig
import os
import platform
import pwd
import random
import shlex
import shutil
import socket
import string
import subprocess
import sys
import tempfile
import time
import errno
import struct
import ssl
from enum import Enum
from typing import Dict, List, Tuple, Optional, Union, Any, NoReturn, Callable, IO, Sequence, TypeVar, cast, Set, Iterable, TextIO, Generator

import re
import uuid

from configparser import ConfigParser
from contextlib import redirect_stdout, contextmanager
from functools import wraps
from glob import glob
from io import StringIO
from threading import Thread, Event
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from pathlib import Path
import cephadmlib.node_proxy.server

FuncT = TypeVar('FuncT', bound=Callable)

# Default container images -----------------------------------------------------
DEFAULT_IMAGE = 'quay.io/ceph/ceph:v18'
DEFAULT_IMAGE_IS_MAIN = False
DEFAULT_IMAGE_RELEASE = 'reef'
DEFAULT_PROMETHEUS_IMAGE = 'quay.io/prometheus/prometheus:v2.43.0'
DEFAULT_LOKI_IMAGE = 'docker.io/grafana/loki:2.4.0'
DEFAULT_PROMTAIL_IMAGE = 'docker.io/grafana/promtail:2.4.0'
DEFAULT_NODE_EXPORTER_IMAGE = 'quay.io/prometheus/node-exporter:v1.5.0'
DEFAULT_ALERT_MANAGER_IMAGE = 'quay.io/prometheus/alertmanager:v0.25.0'
DEFAULT_GRAFANA_IMAGE = 'quay.io/ceph/ceph-grafana:9.4.7'
DEFAULT_HAPROXY_IMAGE = 'quay.io/ceph/haproxy:2.3'
DEFAULT_KEEPALIVED_IMAGE = 'quay.io/ceph/keepalived:2.2.4'
DEFAULT_NVMEOF_IMAGE = 'quay.io/ceph/nvmeof:0.0.1'
DEFAULT_SNMP_GATEWAY_IMAGE = 'docker.io/maxwo/snmp-notifier:v1.2.1'
DEFAULT_ELASTICSEARCH_IMAGE = 'quay.io/omrizeneva/elasticsearch:6.8.23'
DEFAULT_JAEGER_COLLECTOR_IMAGE = 'quay.io/jaegertracing/jaeger-collector:1.29'
DEFAULT_JAEGER_AGENT_IMAGE = 'quay.io/jaegertracing/jaeger-agent:1.29'
DEFAULT_JAEGER_QUERY_IMAGE = 'quay.io/jaegertracing/jaeger-query:1.29'
DEFAULT_REGISTRY = 'docker.io'   # normalize unqualified digests to this
# ------------------------------------------------------------------------------

LATEST_STABLE_RELEASE = 'reef'
DATA_DIR = '/var/lib/ceph'
LOG_DIR = '/var/log/ceph'
LOCK_DIR = '/run/cephadm'
LOGROTATE_DIR = '/etc/logrotate.d'
SYSCTL_DIR = '/etc/sysctl.d'
UNIT_DIR = '/etc/systemd/system'
CEPH_CONF_DIR = 'config'
CEPH_CONF = 'ceph.conf'
CEPH_PUBKEY = 'ceph.pub'
CEPH_KEYRING = 'ceph.client.admin.keyring'
CEPH_DEFAULT_CONF = f'/etc/ceph/{CEPH_CONF}'
CEPH_DEFAULT_KEYRING = f'/etc/ceph/{CEPH_KEYRING}'
CEPH_DEFAULT_PUBKEY = f'/etc/ceph/{CEPH_PUBKEY}'
LOG_DIR_MODE = 0o770
DATA_DIR_MODE = 0o700
DEFAULT_MODE = 0o600
CONTAINER_INIT = True
MIN_PODMAN_VERSION = (2, 0, 2)
CGROUPS_SPLIT_PODMAN_VERSION = (2, 1, 0)
PIDS_LIMIT_UNLIMITED_PODMAN_VERSION = (3, 4, 1)
CUSTOM_PS1 = r'[ceph: \u@\h \W]\$ '
DEFAULT_TIMEOUT = None  # in seconds
DEFAULT_RETRY = 15
DATEFMT = '%Y-%m-%dT%H:%M:%S.%fZ'
QUIET_LOG_LEVEL = 9  # DEBUG is 10, so using 9 to be lower level than DEBUG
NO_DEPRECATED = False

logger: logging.Logger = None  # type: ignore

"""
You can invoke cephadm in two ways:

1. The normal way, at the command line.

2. By piping the script to the python3 binary.  In this latter case, you should
   prepend one or more lines to the beginning of the script.

   For arguments,

       injected_argv = [...]

   e.g.,

       injected_argv = ['ls']

   For reading stdin from the '--config-json -' argument,

       injected_stdin = '...'
"""
cached_stdin = None


##################################


async def run_func(func: Callable, cmd: str) -> subprocess.CompletedProcess:
    logger.debug(f'running function {func.__name__}, with parms: {cmd}')
    response = func(cmd)
    return response


async def concurrent_tasks(func: Callable, cmd_list: List[str]) -> List[Any]:
    tasks = []
    for cmd in cmd_list:
        tasks.append(run_func(func, cmd))

    data = await asyncio.gather(*tasks)

    return data


class EndPoint:
    """EndPoint representing an ip:port format"""

    def __init__(self, ip: str, port: int) -> None:
        self.ip = ip
        self.port = port

    def __str__(self) -> str:
        return f'{self.ip}:{self.port}'

    def __repr__(self) -> str:
        return f'{self.ip}:{self.port}'


class ContainerInfo:
    def __init__(self, container_id: str,
                 image_name: str,
                 image_id: str,
                 start: str,
                 version: str) -> None:
        self.container_id = container_id
        self.image_name = image_name
        self.image_id = image_id
        self.start = start
        self.version = version

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ContainerInfo):
            return NotImplemented
        return (self.container_id == other.container_id
                and self.image_name == other.image_name
                and self.image_id == other.image_id
                and self.start == other.start
                and self.version == other.version)


class DeploymentType(Enum):
    # Fresh deployment of a daemon.
    DEFAULT = 'Deploy'
    # Redeploying a daemon. Works the same as fresh
    # deployment minus port checking.
    REDEPLOY = 'Redeploy'
    # Reconfiguring a daemon. Rewrites config
    # files and potentially restarts daemon.
    RECONFIG = 'Reconfig'


class BaseConfig:

    def __init__(self) -> None:
        self.image: str = ''
        self.docker: bool = False
        self.data_dir: str = DATA_DIR
        self.log_dir: str = LOG_DIR
        self.logrotate_dir: str = LOGROTATE_DIR
        self.sysctl_dir: str = SYSCTL_DIR
        self.unit_dir: str = UNIT_DIR
        self.verbose: bool = False
        self.timeout: Optional[int] = DEFAULT_TIMEOUT
        self.retry: int = DEFAULT_RETRY
        self.env: List[str] = []
        self.memory_request: Optional[int] = None
        self.memory_limit: Optional[int] = None
        self.log_to_journald: Optional[bool] = None

        self.container_init: bool = CONTAINER_INIT
        self.container_engine: Optional[ContainerEngine] = None

    def set_from_args(self, args: argparse.Namespace) -> None:
        argdict: Dict[str, Any] = vars(args)
        for k, v in argdict.items():
            if hasattr(self, k):
                setattr(self, k, v)


class CephadmContext:

    def __init__(self) -> None:
        self.__dict__['_args'] = None
        self.__dict__['_conf'] = BaseConfig()

    def set_args(self, args: argparse.Namespace) -> None:
        self._conf.set_from_args(args)
        self._args = args

    def has_function(self) -> bool:
        return 'func' in self._args

    def __contains__(self, name: str) -> bool:
        return hasattr(self, name)

    def __getattr__(self, name: str) -> Any:
        if '_conf' in self.__dict__ and hasattr(self._conf, name):
            return getattr(self._conf, name)
        elif '_args' in self.__dict__ and hasattr(self._args, name):
            return getattr(self._args, name)
        else:
            return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        if hasattr(self._conf, name):
            setattr(self._conf, name, value)
        elif hasattr(self._args, name):
            setattr(self._args, name, value)
        else:
            super().__setattr__(name, value)


class ContainerEngine:
    def __init__(self) -> None:
        self.path = find_program(self.EXE)

    @property
    def EXE(self) -> str:
        raise NotImplementedError()

    def __str__(self) -> str:
        return f'{self.EXE} ({self.path})'


class Podman(ContainerEngine):
    EXE = 'podman'

    def __init__(self) -> None:
        super().__init__()
        self._version: Optional[Tuple[int, ...]] = None

    @property
    def version(self) -> Tuple[int, ...]:
        if self._version is None:
            raise RuntimeError('Please call `get_version` first')
        return self._version

    def get_version(self, ctx: CephadmContext) -> None:
        out, _, _ = call_throws(ctx, [self.path, 'version', '--format', '{{.Client.Version}}'], verbosity=CallVerbosity.QUIET)
        self._version = _parse_podman_version(out)

    def __str__(self) -> str:
        version = '.'.join(map(str, self.version))
        return f'{self.EXE} ({self.path}) version {version}'


class Docker(ContainerEngine):
    EXE = 'docker'


CONTAINER_PREFERENCE = (Podman, Docker)  # prefer podman to docker


# During normal cephadm operations (cephadm ls, gather-facts, etc ) we use:
# stdout: for JSON output only
# stderr: for error, debug, info, etc
logging_config = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'cephadm': {
            'format': '%(asctime)s %(thread)x %(levelname)s %(message)s'
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
        },
        'log_file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.WatchedFileHandler',
            'formatter': 'cephadm',
            'filename': '%s/cephadm.log' % LOG_DIR,
        }
    },
    'loggers': {
        '': {
            'level': 'DEBUG',
            'handlers': ['console', 'log_file'],
        }
    }
}


class ExcludeErrorsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        """Only lets through log messages with log level below WARNING ."""
        return record.levelno < logging.WARNING


# When cephadm is used as standard binary (bootstrap, rm-cluster, etc) we use:
# stdout: for debug and info
# stderr: for errors and warnings
interactive_logging_config = {
    'version': 1,
    'filters': {
        'exclude_errors': {
            '()': ExcludeErrorsFilter
        }
    },
    'disable_existing_loggers': True,
    'formatters': {
        'cephadm': {
            'format': '%(asctime)s %(thread)x %(levelname)s %(message)s'
        },
    },
    'handlers': {
        'console_stdout': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'filters': ['exclude_errors'],
            'stream': sys.stdout
        },
        'console_stderr': {
            'level': 'WARNING',
            'class': 'logging.StreamHandler',
            'stream': sys.stderr
        },
        'log_file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.WatchedFileHandler',
            'formatter': 'cephadm',
            'filename': '%s/cephadm.log' % LOG_DIR,
        }
    },
    'loggers': {
        '': {
            'level': 'DEBUG',
            'handlers': ['console_stdout', 'console_stderr', 'log_file'],
        }
    }
}


class termcolor:
    yellow = '\033[93m'
    red = '\033[31m'
    end = '\033[0m'


class Error(Exception):
    pass


class ClusterAlreadyExists(Exception):
    pass


class TimeoutExpired(Error):
    pass


class UnauthorizedRegistryError(Error):
    pass

##################################


class Ceph(object):
    daemons = ('mon', 'mgr', 'osd', 'mds', 'rgw', 'rbd-mirror',
               'crash', 'cephfs-mirror', 'ceph-exporter')
    gateways = ('iscsi', 'nfs', 'nvmeof')

##################################


class OSD(object):
    @staticmethod
    def get_sysctl_settings() -> List[str]:
        return [
            '# allow a large number of OSDs',
            'fs.aio-max-nr = 1048576',
            'kernel.pid_max = 4194304',
        ]


##################################


class SNMPGateway:
    """Defines an SNMP gateway between Prometheus and SNMP monitoring Frameworks"""
    daemon_type = 'snmp-gateway'
    SUPPORTED_VERSIONS = ['V2c', 'V3']
    default_image = DEFAULT_SNMP_GATEWAY_IMAGE
    DEFAULT_PORT = 9464
    env_filename = 'snmp-gateway.conf'

    def __init__(self,
                 ctx: CephadmContext,
                 fsid: str,
                 daemon_id: Union[int, str],
                 config_json: Dict[str, Any],
                 image: Optional[str] = None) -> None:
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image or SNMPGateway.default_image

        self.uid = config_json.get('uid', 0)
        self.gid = config_json.get('gid', 0)

        self.destination = config_json.get('destination', '')
        self.snmp_version = config_json.get('snmp_version', 'V2c')
        self.snmp_community = config_json.get('snmp_community', 'public')
        self.log_level = config_json.get('log_level', 'info')
        self.snmp_v3_auth_username = config_json.get('snmp_v3_auth_username', '')
        self.snmp_v3_auth_password = config_json.get('snmp_v3_auth_password', '')
        self.snmp_v3_auth_protocol = config_json.get('snmp_v3_auth_protocol', '')
        self.snmp_v3_priv_protocol = config_json.get('snmp_v3_priv_protocol', '')
        self.snmp_v3_priv_password = config_json.get('snmp_v3_priv_password', '')
        self.snmp_v3_engine_id = config_json.get('snmp_v3_engine_id', '')

        self.validate()

    @classmethod
    def init(cls, ctx: CephadmContext, fsid: str,
             daemon_id: Union[int, str]) -> 'SNMPGateway':
        cfgs = fetch_configs(ctx)
        assert cfgs  # assert some config data was found
        return cls(ctx, fsid, daemon_id, cfgs, ctx.image)

    @staticmethod
    def get_version(ctx: CephadmContext, fsid: str, daemon_id: str) -> Optional[str]:
        """Return the version of the notifier from it's http endpoint"""
        path = os.path.join(ctx.data_dir, fsid, f'snmp-gateway.{daemon_id}', 'unit.meta')
        try:
            with open(path, 'r') as env:
                metadata = json.loads(env.read())
        except (OSError, json.JSONDecodeError):
            return None

        ports = metadata.get('ports', [])
        if not ports:
            return None

        try:
            with urlopen(f'http://127.0.0.1:{ports[0]}/') as r:
                html = r.read().decode('utf-8').split('\n')
        except (HTTPError, URLError):
            return None

        for h in html:
            stripped = h.strip()
            if stripped.startswith(('<pre>', '<PRE>')) and \
               stripped.endswith(('</pre>', '</PRE>')):
                # <pre>(version=1.2.1, branch=HEAD, revision=7...
                return stripped.split(',')[0].split('version=')[1]

        return None

    @property
    def port(self) -> int:
        endpoints = fetch_tcp_ports(self.ctx)
        if not endpoints:
            return self.DEFAULT_PORT
        return endpoints[0].port

    def get_daemon_args(self) -> List[str]:
        v3_args = []
        base_args = [
            f'--web.listen-address=:{self.port}',
            f'--snmp.destination={self.destination}',
            f'--snmp.version={self.snmp_version}',
            f'--log.level={self.log_level}',
            '--snmp.trap-description-template=/etc/snmp_notifier/description-template.tpl'
        ]

        if self.snmp_version == 'V3':
            # common auth settings
            v3_args.extend([
                '--snmp.authentication-enabled',
                f'--snmp.authentication-protocol={self.snmp_v3_auth_protocol}',
                f'--snmp.security-engine-id={self.snmp_v3_engine_id}'
            ])
            # authPriv setting is applied if we have a privacy protocol setting
            if self.snmp_v3_priv_protocol:
                v3_args.extend([
                    '--snmp.private-enabled',
                    f'--snmp.private-protocol={self.snmp_v3_priv_protocol}'
                ])

        return base_args + v3_args

    @property
    def data_dir(self) -> str:
        return os.path.join(self.ctx.data_dir, self.ctx.fsid, f'{self.daemon_type}.{self.daemon_id}')

    @property
    def conf_file_path(self) -> str:
        return os.path.join(self.data_dir, self.env_filename)

    def create_daemon_conf(self) -> None:
        """Creates the environment file holding 'secrets' passed to the snmp-notifier daemon"""
        with write_new(self.conf_file_path) as f:
            if self.snmp_version == 'V2c':
                f.write(f'SNMP_NOTIFIER_COMMUNITY={self.snmp_community}\n')
            else:
                f.write(f'SNMP_NOTIFIER_AUTH_USERNAME={self.snmp_v3_auth_username}\n')
                f.write(f'SNMP_NOTIFIER_AUTH_PASSWORD={self.snmp_v3_auth_password}\n')
                if self.snmp_v3_priv_password:
                    f.write(f'SNMP_NOTIFIER_PRIV_PASSWORD={self.snmp_v3_priv_password}\n')

    def validate(self) -> None:
        """Validate the settings

        Raises:
            Error: if the fsid doesn't look like an fsid
            Error: if the snmp version is not supported
            Error: destination IP and port address missing
        """
        if not is_fsid(self.fsid):
            raise Error(f'not a valid fsid: {self.fsid}')

        if self.snmp_version not in SNMPGateway.SUPPORTED_VERSIONS:
            raise Error(f'not a valid snmp version: {self.snmp_version}')

        if not self.destination:
            raise Error('config is missing destination attribute(<ip>:<port>) of the target SNMP listener')


##################################
class Monitoring(object):
    """Define the configs for the monitoring containers"""

    port_map = {
        'prometheus': [9095],  # Avoid default 9090, due to conflict with cockpit UI
        'node-exporter': [9100],
        'grafana': [3000],
        'alertmanager': [9093, 9094],
        'loki': [3100],
        'promtail': [9080]
    }

    components = {
        'prometheus': {
            'image': DEFAULT_PROMETHEUS_IMAGE,
            'cpus': '2',
            'memory': '4GB',
            'args': [
                '--config.file=/etc/prometheus/prometheus.yml',
                '--storage.tsdb.path=/prometheus',
            ],
            'config-json-files': [
                'prometheus.yml',
            ],
        },
        'loki': {
            'image': DEFAULT_LOKI_IMAGE,
            'cpus': '1',
            'memory': '1GB',
            'args': [
                '--config.file=/etc/loki/loki.yml',
            ],
            'config-json-files': [
                'loki.yml'
            ],
        },
        'promtail': {
            'image': DEFAULT_PROMTAIL_IMAGE,
            'cpus': '1',
            'memory': '1GB',
            'args': [
                '--config.file=/etc/promtail/promtail.yml',
            ],
            'config-json-files': [
                'promtail.yml',
            ],
        },
        'node-exporter': {
            'image': DEFAULT_NODE_EXPORTER_IMAGE,
            'cpus': '1',
            'memory': '1GB',
            'args': [
                '--no-collector.timex'
            ],
        },
        'grafana': {
            'image': DEFAULT_GRAFANA_IMAGE,
            'cpus': '2',
            'memory': '4GB',
            'args': [],
            'config-json-files': [
                'grafana.ini',
                'provisioning/datasources/ceph-dashboard.yml',
                'certs/cert_file',
                'certs/cert_key',
            ],
        },
        'alertmanager': {
            'image': DEFAULT_ALERT_MANAGER_IMAGE,
            'cpus': '2',
            'memory': '2GB',
            'args': [
                '--cluster.listen-address=:{}'.format(port_map['alertmanager'][1]),
            ],
            'config-json-files': [
                'alertmanager.yml',
            ],
            'config-json-args': [
                'peers',
            ],
        },
    }  # type: ignore

    @staticmethod
    def get_version(ctx, container_id, daemon_type):
        # type: (CephadmContext, str, str) -> str
        """
        :param: daemon_type Either "prometheus", "alertmanager", "loki", "promtail" or "node-exporter"
        """
        assert daemon_type in ('prometheus', 'alertmanager', 'node-exporter', 'loki', 'promtail')
        cmd = daemon_type.replace('-', '_')
        code = -1
        err = ''
        out = ''
        version = ''
        if daemon_type == 'alertmanager':
            for cmd in ['alertmanager', 'prometheus-alertmanager']:
                out, err, code = call(ctx, [
                    ctx.container_engine.path, 'exec', container_id, cmd,
                    '--version'
                ], verbosity=CallVerbosity.QUIET)
                if code == 0:
                    break
            cmd = 'alertmanager'  # reset cmd for version extraction
        else:
            out, err, code = call(ctx, [
                ctx.container_engine.path, 'exec', container_id, cmd, '--version'
            ], verbosity=CallVerbosity.QUIET)
        if code == 0:
            if err.startswith('%s, version ' % cmd):
                version = err.split(' ')[2]
            elif out.startswith('%s, version ' % cmd):
                version = out.split(' ')[2]
        return version

##################################


@contextmanager
def write_new(
    destination: Union[str, Path],
    *,
    owner: Optional[Tuple[int, int]] = None,
    perms: Optional[int] = DEFAULT_MODE,
    encoding: Optional[str] = None,
) -> Generator[IO, None, None]:
    """Write a new file in a robust manner, optionally specifying the owner,
    permissions, or encoding. This function takes care to never leave a file in
    a partially-written state due to a crash or power outage by writing to
    temporary file and then renaming that temp file over to the final
    destination once all data is written.  Note that the temporary files can be
    leaked but only for a "crash" or power outage - regular exceptions will
    clean up the temporary file.
    """
    destination = os.path.abspath(destination)
    tempname = f'{destination}.new'
    open_kwargs: Dict[str, Any] = {}
    if encoding:
        open_kwargs['encoding'] = encoding
    try:
        with open(tempname, 'w', **open_kwargs) as fh:
            yield fh
            fh.flush()
            os.fsync(fh.fileno())
            if owner is not None:
                os.fchown(fh.fileno(), *owner)
            if perms is not None:
                os.fchmod(fh.fileno(), perms)
    except Exception:
        os.unlink(tempname)
        raise
    os.rename(tempname, destination)


def populate_files(config_dir, config_files, uid, gid):
    # type: (str, Dict, int, int) -> None
    """create config files for different services"""
    for fname in config_files:
        config_file = os.path.join(config_dir, fname)
        config_content = dict_get_join(config_files, fname)
        logger.info('Write file: %s' % (config_file))
        with write_new(config_file, owner=(uid, gid), encoding='utf-8') as f:
            f.write(config_content)


class NFSGanesha(object):
    """Defines a NFS-Ganesha container"""

    daemon_type = 'nfs'
    entrypoint = '/usr/bin/ganesha.nfsd'
    daemon_args = ['-F', '-L', 'STDERR']

    required_files = ['ganesha.conf']

    port_map = {
        'nfs': 2049,
    }

    def __init__(self,
                 ctx,
                 fsid,
                 daemon_id,
                 config_json,
                 image=DEFAULT_IMAGE):
        # type: (CephadmContext, str, Union[int, str], Dict, str) -> None
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        # config-json options
        self.pool = dict_get(config_json, 'pool', require=True)
        self.namespace = dict_get(config_json, 'namespace')
        self.userid = dict_get(config_json, 'userid')
        self.extra_args = dict_get(config_json, 'extra_args', [])
        self.files = dict_get(config_json, 'files', {})
        self.rgw = dict_get(config_json, 'rgw', {})

        # validate the supplied args
        self.validate()

    @classmethod
    def init(cls, ctx, fsid, daemon_id):
        # type: (CephadmContext, str, Union[int, str]) -> NFSGanesha
        return cls(ctx, fsid, daemon_id, fetch_configs(ctx), ctx.image)

    def get_container_mounts(self, data_dir):
        # type: (str) -> Dict[str, str]
        mounts = dict()
        mounts[os.path.join(data_dir, 'config')] = '/etc/ceph/ceph.conf:z'
        mounts[os.path.join(data_dir, 'keyring')] = '/etc/ceph/keyring:z'
        mounts[os.path.join(data_dir, 'etc/ganesha')] = '/etc/ganesha:z'
        if self.rgw:
            cluster = self.rgw.get('cluster', 'ceph')
            rgw_user = self.rgw.get('user', 'admin')
            mounts[os.path.join(data_dir, 'keyring.rgw')] = \
                '/var/lib/ceph/radosgw/%s-%s/keyring:z' % (cluster, rgw_user)
        return mounts

    @staticmethod
    def get_container_envs():
        # type: () -> List[str]
        envs = [
            'CEPH_CONF=%s' % (CEPH_DEFAULT_CONF)
        ]
        return envs

    @staticmethod
    def get_version(ctx, container_id):
        # type: (CephadmContext, str) -> Optional[str]
        version = None
        out, err, code = call(ctx,
                              [ctx.container_engine.path, 'exec', container_id,
                               NFSGanesha.entrypoint, '-v'],
                              verbosity=CallVerbosity.QUIET)
        if code == 0:
            match = re.search(r'NFS-Ganesha Release\s*=\s*[V]*([\d.]+)', out)
            if match:
                version = match.group(1)
        return version

    def validate(self):
        # type: () -> None
        if not is_fsid(self.fsid):
            raise Error('not an fsid: %s' % self.fsid)
        if not self.daemon_id:
            raise Error('invalid daemon_id: %s' % self.daemon_id)
        if not self.image:
            raise Error('invalid image: %s' % self.image)

        # check for the required files
        if self.required_files:
            for fname in self.required_files:
                if fname not in self.files:
                    raise Error('required file missing from config-json: %s' % fname)

        # check for an RGW config
        if self.rgw:
            if not self.rgw.get('keyring'):
                raise Error('RGW keyring is missing')
            if not self.rgw.get('user'):
                raise Error('RGW user is missing')

    def get_daemon_name(self):
        # type: () -> str
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def get_container_name(self, desc=None):
        # type: (Optional[str]) -> str
        cname = 'ceph-%s-%s' % (self.fsid, self.get_daemon_name())
        if desc:
            cname = '%s-%s' % (cname, desc)
        return cname

    def get_daemon_args(self):
        # type: () -> List[str]
        return self.daemon_args + self.extra_args

    def create_daemon_dirs(self, data_dir, uid, gid):
        # type: (str, int, int) -> None
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))

        logger.info('Creating ganesha config...')

        # create the ganesha conf dir
        config_dir = os.path.join(data_dir, 'etc/ganesha')
        makedirs(config_dir, uid, gid, 0o755)

        # populate files from the config-json
        populate_files(config_dir, self.files, uid, gid)

        # write the RGW keyring
        if self.rgw:
            keyring_path = os.path.join(data_dir, 'keyring.rgw')
            with write_new(keyring_path, owner=(uid, gid)) as f:
                f.write(self.rgw.get('keyring', ''))

##################################


class CephIscsi(object):
    """Defines a Ceph-Iscsi container"""

    daemon_type = 'iscsi'
    entrypoint = '/usr/bin/rbd-target-api'

    required_files = ['iscsi-gateway.cfg']

    def __init__(self,
                 ctx,
                 fsid,
                 daemon_id,
                 config_json,
                 image=DEFAULT_IMAGE):
        # type: (CephadmContext, str, Union[int, str], Dict, str) -> None
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        # config-json options
        self.files = dict_get(config_json, 'files', {})

        # validate the supplied args
        self.validate()

    @classmethod
    def init(cls, ctx, fsid, daemon_id):
        # type: (CephadmContext, str, Union[int, str]) -> CephIscsi
        return cls(ctx, fsid, daemon_id,
                   fetch_configs(ctx), ctx.image)

    @staticmethod
    def get_container_mounts(data_dir, log_dir):
        # type: (str, str) -> Dict[str, str]
        mounts = dict()
        mounts[os.path.join(data_dir, 'config')] = '/etc/ceph/ceph.conf:z'
        mounts[os.path.join(data_dir, 'keyring')] = '/etc/ceph/keyring:z'
        mounts[os.path.join(data_dir, 'iscsi-gateway.cfg')] = '/etc/ceph/iscsi-gateway.cfg:z'
        mounts[os.path.join(data_dir, 'configfs')] = '/sys/kernel/config'
        mounts[os.path.join(data_dir, 'tcmu-runner-entrypoint.sh')] = '/usr/local/scripts/tcmu-runner-entrypoint.sh'
        mounts[log_dir] = '/var/log:z'
        mounts['/dev'] = '/dev'
        return mounts

    @staticmethod
    def get_container_binds():
        # type: () -> List[List[str]]
        binds = []
        lib_modules = ['type=bind',
                       'source=/lib/modules',
                       'destination=/lib/modules',
                       'ro=true']
        binds.append(lib_modules)
        return binds

    @staticmethod
    def get_version(ctx, container_id):
        # type: (CephadmContext, str) -> Optional[str]
        version = None
        out, err, code = call(ctx,
                              [ctx.container_engine.path, 'exec', container_id,
                               '/usr/bin/python3', '-c',
                               "import pkg_resources; print(pkg_resources.require('ceph_iscsi')[0].version)"],
                              verbosity=CallVerbosity.QUIET)
        if code == 0:
            version = out.strip()
        return version

    def validate(self):
        # type: () -> None
        if not is_fsid(self.fsid):
            raise Error('not an fsid: %s' % self.fsid)
        if not self.daemon_id:
            raise Error('invalid daemon_id: %s' % self.daemon_id)
        if not self.image:
            raise Error('invalid image: %s' % self.image)

        # check for the required files
        if self.required_files:
            for fname in self.required_files:
                if fname not in self.files:
                    raise Error('required file missing from config-json: %s' % fname)

    def get_daemon_name(self):
        # type: () -> str
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def get_container_name(self, desc=None):
        # type: (Optional[str]) -> str
        cname = 'ceph-%s-%s' % (self.fsid, self.get_daemon_name())
        if desc:
            cname = '%s-%s' % (cname, desc)
        return cname

    def create_daemon_dirs(self, data_dir, uid, gid):
        # type: (str, int, int) -> None
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))

        logger.info('Creating ceph-iscsi config...')
        configfs_dir = os.path.join(data_dir, 'configfs')
        makedirs(configfs_dir, uid, gid, 0o755)

        # set up the tcmu-runner entrypoint script
        # to be mounted into the container. For more info
        # on why we need this script, see the
        # tcmu_runner_entrypoint_script function
        self.files['tcmu-runner-entrypoint.sh'] = self.tcmu_runner_entrypoint_script()

        # populate files from the config-json
        populate_files(data_dir, self.files, uid, gid)

        # we want the tcmu runner entrypoint script to be executable
        # populate_files will give it 0o600 by default
        os.chmod(os.path.join(data_dir, 'tcmu-runner-entrypoint.sh'), 0o700)

    @staticmethod
    def configfs_mount_umount(data_dir, mount=True):
        # type: (str, bool) -> List[str]
        mount_path = os.path.join(data_dir, 'configfs')
        if mount:
            cmd = 'if ! grep -qs {0} /proc/mounts; then ' \
                  'mount -t configfs none {0}; fi'.format(mount_path)
        else:
            cmd = 'if grep -qs {0} /proc/mounts; then ' \
                  'umount {0}; fi'.format(mount_path)
        return cmd.split()

    @staticmethod
    def tcmu_runner_entrypoint_script() -> str:
        # since we are having tcmu-runner be a background
        # process in its systemd unit (rbd-target-api being
        # the main process) systemd will not restart it when
        # it fails. in order to try and get around that for now
        # we can have a script mounted in the container that
        # that attempts to do the restarting for us. This script
        # can then become the entrypoint for the tcmu-runner
        # container

        # This is intended to be dropped for a better solution
        # for at least the squid release onward
        return """#!/bin/bash
RUN_DIR=/var/run/tcmu-runner

if [ ! -d "${RUN_DIR}" ] ; then
    mkdir -p "${RUN_DIR}"
fi

rm -rf "${RUN_DIR}"/*

while true
do
    touch "${RUN_DIR}"/start-up-$(date -Ins)
    /usr/bin/tcmu-runner

    # If we got around 3 kills/segfaults in the last minute,
    # don't start anymore
    if [ $(find "${RUN_DIR}" -type f -cmin -1 | wc -l) -ge 3 ] ; then
        exit 0
    fi

    sleep 1
done
"""

    def get_tcmu_runner_container(self):
        # type: () -> CephContainer
        # daemon_id, is used to generated the cid and pid files used by podman but as both tcmu-runner
        # and rbd-target-api have the same daemon_id, it conflits and prevent the second container from
        # starting. .tcmu runner is appended to the daemon_id to fix that.
        tcmu_container = get_deployment_container(self.ctx, self.fsid, self.daemon_type, str(self.daemon_id) + '.tcmu')
        # TODO: Eventually we don't want to run tcmu-runner through this script.
        # This is intended to be a workaround backported to older releases
        # and should eventually be removed in at least squid onward
        tcmu_container.entrypoint = '/usr/local/scripts/tcmu-runner-entrypoint.sh'
        tcmu_container.cname = self.get_container_name(desc='tcmu')
        return tcmu_container


##################################


class CephNvmeof(object):
    """Defines a Ceph-Nvmeof container"""

    daemon_type = 'nvmeof'
    required_files = ['ceph-nvmeof.conf']
    default_image = DEFAULT_NVMEOF_IMAGE

    def __init__(self,
                 ctx,
                 fsid,
                 daemon_id,
                 config_json,
                 image=DEFAULT_NVMEOF_IMAGE):
        # type: (CephadmContext, str, Union[int, str], Dict, str) -> None
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        # config-json options
        self.files = dict_get(config_json, 'files', {})

        # validate the supplied args
        self.validate()

    @classmethod
    def init(cls, ctx, fsid, daemon_id):
        # type: (CephadmContext, str, Union[int, str]) -> CephNvmeof
        return cls(ctx, fsid, daemon_id,
                   fetch_configs(ctx), ctx.image)

    @staticmethod
    def get_container_mounts(data_dir: str) -> Dict[str, str]:
        mounts = dict()
        mounts[os.path.join(data_dir, 'config')] = '/etc/ceph/ceph.conf:z'
        mounts[os.path.join(data_dir, 'keyring')] = '/etc/ceph/keyring:z'
        mounts[os.path.join(data_dir, 'ceph-nvmeof.conf')] = '/src/ceph-nvmeof.conf:z'
        mounts[os.path.join(data_dir, 'configfs')] = '/sys/kernel/config'
        mounts['/dev/hugepages'] = '/dev/hugepages'
        mounts['/dev/vfio/vfio'] = '/dev/vfio/vfio'
        return mounts

    @staticmethod
    def get_container_binds():
        # type: () -> List[List[str]]
        binds = []
        lib_modules = ['type=bind',
                       'source=/lib/modules',
                       'destination=/lib/modules',
                       'ro=true']
        binds.append(lib_modules)
        return binds

    @staticmethod
    def get_version(ctx: CephadmContext, container_id: str) -> Optional[str]:
        out, err, ret = call(ctx,
                             [ctx.container_engine.path, 'inspect',
                              '--format', '{{index .Config.Labels "io.ceph.version"}}',
                              ctx.image])
        version = None
        if ret == 0:
            version = out.strip()
        return version

    def validate(self):
        # type: () -> None
        if not is_fsid(self.fsid):
            raise Error('not an fsid: %s' % self.fsid)
        if not self.daemon_id:
            raise Error('invalid daemon_id: %s' % self.daemon_id)
        if not self.image:
            raise Error('invalid image: %s' % self.image)

        # check for the required files
        if self.required_files:
            for fname in self.required_files:
                if fname not in self.files:
                    raise Error('required file missing from config-json: %s' % fname)

    def get_daemon_name(self):
        # type: () -> str
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def get_container_name(self, desc=None):
        # type: (Optional[str]) -> str
        cname = '%s-%s' % (self.fsid, self.get_daemon_name())
        if desc:
            cname = '%s-%s' % (cname, desc)
        return cname

    def create_daemon_dirs(self, data_dir, uid, gid):
        # type: (str, int, int) -> None
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))

        logger.info('Creating ceph-nvmeof config...')
        configfs_dir = os.path.join(data_dir, 'configfs')
        makedirs(configfs_dir, uid, gid, 0o755)

        # populate files from the config-json
        populate_files(data_dir, self.files, uid, gid)

    @staticmethod
    def configfs_mount_umount(data_dir, mount=True):
        # type: (str, bool) -> List[str]
        mount_path = os.path.join(data_dir, 'configfs')
        if mount:
            cmd = 'if ! grep -qs {0} /proc/mounts; then ' \
                  'mount -t configfs none {0}; fi'.format(mount_path)
        else:
            cmd = 'if grep -qs {0} /proc/mounts; then ' \
                  'umount {0}; fi'.format(mount_path)
        return cmd.split()

    @staticmethod
    def get_sysctl_settings() -> List[str]:
        return [
            'vm.nr_hugepages = 4096',
        ]


##################################


class CephExporter(object):
    """Defines a Ceph exporter container"""

    daemon_type = 'ceph-exporter'
    entrypoint = '/usr/bin/ceph-exporter'
    DEFAULT_PORT = 9926
    port_map = {
        'ceph-exporter': DEFAULT_PORT,
    }

    def __init__(self,
                 ctx: CephadmContext,
                 fsid: str, daemon_id: Union[int, str],
                 config_json: Dict[str, Any],
                 image: str = DEFAULT_IMAGE) -> None:
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        self.sock_dir = config_json.get('sock-dir', '/var/run/ceph/')
        ipv4_addrs, _ = get_ip_addresses(get_hostname())
        addrs = '0.0.0.0' if ipv4_addrs else '::'
        self.addrs = config_json.get('addrs', addrs)
        self.port = config_json.get('port', self.DEFAULT_PORT)
        self.prio_limit = config_json.get('prio-limit', 5)
        self.stats_period = config_json.get('stats-period', 5)

        self.validate()

    @classmethod
    def init(cls, ctx: CephadmContext, fsid: str,
             daemon_id: Union[int, str]) -> 'CephExporter':
        return cls(ctx, fsid, daemon_id,
                   fetch_configs(ctx), ctx.image)

    @staticmethod
    def get_container_mounts() -> Dict[str, str]:
        mounts = dict()
        mounts['/var/run/ceph'] = '/var/run/ceph:z'
        return mounts

    def get_daemon_args(self) -> List[str]:
        args = [
            f'--sock-dir={self.sock_dir}',
            f'--addrs={self.addrs}',
            f'--port={self.port}',
            f'--prio-limit={self.prio_limit}',
            f'--stats-period={self.stats_period}',
        ]
        return args

    def validate(self) -> None:
        if not os.path.isdir(self.sock_dir):
            raise Error(f'Directory does not exist. Got: {self.sock_dir}')


##################################


class HAproxy(object):
    """Defines an HAproxy container"""
    daemon_type = 'haproxy'
    required_files = ['haproxy.cfg']
    default_image = DEFAULT_HAPROXY_IMAGE

    def __init__(self,
                 ctx: CephadmContext,
                 fsid: str, daemon_id: Union[int, str],
                 config_json: Dict, image: str) -> None:
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        # config-json options
        self.files = dict_get(config_json, 'files', {})

        self.validate()

    @classmethod
    def init(cls, ctx: CephadmContext,
             fsid: str, daemon_id: Union[int, str]) -> 'HAproxy':
        return cls(ctx, fsid, daemon_id, fetch_configs(ctx),
                   ctx.image)

    def create_daemon_dirs(self, data_dir: str, uid: int, gid: int) -> None:
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))

        # create additional directories in data dir for HAproxy to use
        if not os.path.isdir(os.path.join(data_dir, 'haproxy')):
            makedirs(os.path.join(data_dir, 'haproxy'), uid, gid, DATA_DIR_MODE)

        data_dir = os.path.join(data_dir, 'haproxy')
        populate_files(data_dir, self.files, uid, gid)

    def get_daemon_args(self) -> List[str]:
        return ['haproxy', '-f', '/var/lib/haproxy/haproxy.cfg']

    def validate(self):
        # type: () -> None
        if not is_fsid(self.fsid):
            raise Error('not an fsid: %s' % self.fsid)
        if not self.daemon_id:
            raise Error('invalid daemon_id: %s' % self.daemon_id)
        if not self.image:
            raise Error('invalid image: %s' % self.image)

        # check for the required files
        if self.required_files:
            for fname in self.required_files:
                if fname not in self.files:
                    raise Error('required file missing from config-json: %s' % fname)

    def get_daemon_name(self):
        # type: () -> str
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def get_container_name(self, desc=None):
        # type: (Optional[str]) -> str
        cname = 'ceph-%s-%s' % (self.fsid, self.get_daemon_name())
        if desc:
            cname = '%s-%s' % (cname, desc)
        return cname

    def extract_uid_gid_haproxy(self) -> Tuple[int, int]:
        # better directory for this?
        return extract_uid_gid(self.ctx, file_path='/var/lib')

    @staticmethod
    def get_container_mounts(data_dir: str) -> Dict[str, str]:
        mounts = dict()
        mounts[os.path.join(data_dir, 'haproxy')] = '/var/lib/haproxy'
        return mounts

    @staticmethod
    def get_sysctl_settings() -> List[str]:
        return [
            '# IP forwarding and non-local bind',
            'net.ipv4.ip_forward = 1',
            'net.ipv4.ip_nonlocal_bind = 1',
        ]

##################################


class Keepalived(object):
    """Defines an Keepalived container"""
    daemon_type = 'keepalived'
    required_files = ['keepalived.conf']
    default_image = DEFAULT_KEEPALIVED_IMAGE

    def __init__(self,
                 ctx: CephadmContext,
                 fsid: str, daemon_id: Union[int, str],
                 config_json: Dict, image: str) -> None:
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        # config-json options
        self.files = dict_get(config_json, 'files', {})

        self.validate()

    @classmethod
    def init(cls, ctx: CephadmContext, fsid: str,
             daemon_id: Union[int, str]) -> 'Keepalived':
        return cls(ctx, fsid, daemon_id,
                   fetch_configs(ctx), ctx.image)

    def create_daemon_dirs(self, data_dir: str, uid: int, gid: int) -> None:
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))

        # create additional directories in data dir for keepalived to use
        if not os.path.isdir(os.path.join(data_dir, 'keepalived')):
            makedirs(os.path.join(data_dir, 'keepalived'), uid, gid, DATA_DIR_MODE)

        # populate files from the config-json
        populate_files(data_dir, self.files, uid, gid)

    def validate(self):
        # type: () -> None
        if not is_fsid(self.fsid):
            raise Error('not an fsid: %s' % self.fsid)
        if not self.daemon_id:
            raise Error('invalid daemon_id: %s' % self.daemon_id)
        if not self.image:
            raise Error('invalid image: %s' % self.image)

        # check for the required files
        if self.required_files:
            for fname in self.required_files:
                if fname not in self.files:
                    raise Error('required file missing from config-json: %s' % fname)

    def get_daemon_name(self):
        # type: () -> str
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def get_container_name(self, desc=None):
        # type: (Optional[str]) -> str
        cname = 'ceph-%s-%s' % (self.fsid, self.get_daemon_name())
        if desc:
            cname = '%s-%s' % (cname, desc)
        return cname

    @staticmethod
    def get_container_envs():
        # type: () -> List[str]
        envs = [
            'KEEPALIVED_AUTOCONF=false',
            'KEEPALIVED_CONF=/etc/keepalived/keepalived.conf',
            'KEEPALIVED_CMD=/usr/sbin/keepalived -n -l -f /etc/keepalived/keepalived.conf',
            'KEEPALIVED_DEBUG=false'
        ]
        return envs

    @staticmethod
    def get_sysctl_settings() -> List[str]:
        return [
            '# IP forwarding and non-local bind',
            'net.ipv4.ip_forward = 1',
            'net.ipv4.ip_nonlocal_bind = 1',
        ]

    def extract_uid_gid_keepalived(self) -> Tuple[int, int]:
        # better directory for this?
        return extract_uid_gid(self.ctx, file_path='/var/lib')

    @staticmethod
    def get_container_mounts(data_dir: str) -> Dict[str, str]:
        mounts = dict()
        mounts[os.path.join(data_dir, 'keepalived.conf')] = '/etc/keepalived/keepalived.conf'
        return mounts

##################################


class Tracing(object):
    """Define the configs for the jaeger tracing containers"""

    components: Dict[str, Dict[str, Any]] = {
        'elasticsearch': {
            'image': DEFAULT_ELASTICSEARCH_IMAGE,
            'envs': ['discovery.type=single-node']
        },
        'jaeger-agent': {
            'image': DEFAULT_JAEGER_AGENT_IMAGE,
        },
        'jaeger-collector': {
            'image': DEFAULT_JAEGER_COLLECTOR_IMAGE,
        },
        'jaeger-query': {
            'image': DEFAULT_JAEGER_QUERY_IMAGE,
        },
    }  # type: ignore

    @staticmethod
    def set_configuration(config: Dict[str, str], daemon_type: str) -> None:
        if daemon_type in ['jaeger-collector', 'jaeger-query']:
            assert 'elasticsearch_nodes' in config
            Tracing.components[daemon_type]['envs'] = [
                'SPAN_STORAGE_TYPE=elasticsearch',
                f'ES_SERVER_URLS={config["elasticsearch_nodes"]}']
        if daemon_type == 'jaeger-agent':
            assert 'collector_nodes' in config
            Tracing.components[daemon_type]['daemon_args'] = [
                f'--reporter.grpc.host-port={config["collector_nodes"]}',
                '--processor.jaeger-compact.server-host-port=6799'
            ]

##################################


class CustomContainer(object):
    """Defines a custom container"""
    daemon_type = 'container'

    def __init__(self,
                 fsid: str, daemon_id: Union[int, str],
                 config_json: Dict, image: str) -> None:
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        # config-json options
        self.entrypoint = dict_get(config_json, 'entrypoint')
        self.uid = dict_get(config_json, 'uid', 65534)  # nobody
        self.gid = dict_get(config_json, 'gid', 65534)  # nobody
        self.volume_mounts = dict_get(config_json, 'volume_mounts', {})
        self.args = dict_get(config_json, 'args', [])
        self.envs = dict_get(config_json, 'envs', [])
        self.privileged = dict_get(config_json, 'privileged', False)
        self.bind_mounts = dict_get(config_json, 'bind_mounts', [])
        self.ports = dict_get(config_json, 'ports', [])
        self.dirs = dict_get(config_json, 'dirs', [])
        self.files = dict_get(config_json, 'files', {})

    @classmethod
    def init(cls, ctx: CephadmContext,
             fsid: str, daemon_id: Union[int, str]) -> 'CustomContainer':
        return cls(fsid, daemon_id,
                   fetch_configs(ctx), ctx.image)

    def create_daemon_dirs(self, data_dir: str, uid: int, gid: int) -> None:
        """
        Create dirs/files below the container data directory.
        """
        logger.info('Creating custom container configuration '
                    'dirs/files in {} ...'.format(data_dir))

        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % data_dir)

        for dir_path in self.dirs:
            logger.info('Creating directory: {}'.format(dir_path))
            dir_path = os.path.join(data_dir, dir_path.strip('/'))
            makedirs(dir_path, uid, gid, 0o755)

        for file_path in self.files:
            logger.info('Creating file: {}'.format(file_path))
            content = dict_get_join(self.files, file_path)
            file_path = os.path.join(data_dir, file_path.strip('/'))
            with write_new(file_path, owner=(uid, gid), encoding='utf-8') as f:
                f.write(content)

    def get_daemon_args(self) -> List[str]:
        return []

    def get_container_args(self) -> List[str]:
        return self.args

    def get_container_envs(self) -> List[str]:
        return self.envs

    def get_container_mounts(self, data_dir: str) -> Dict[str, str]:
        """
        Get the volume mounts. Relative source paths will be located below
        `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.

        Example:
        {
            /foo/conf: /conf
            foo/conf: /conf
        }
        becomes
        {
            /foo/conf: /conf
            /var/lib/ceph/<cluster-fsid>/<daemon-name>/foo/conf: /conf
        }
        """
        mounts = {}
        for source, destination in self.volume_mounts.items():
            source = os.path.join(data_dir, source)
            mounts[source] = destination
        return mounts

    def get_container_binds(self, data_dir: str) -> List[List[str]]:
        """
        Get the bind mounts. Relative `source=...` paths will be located below
        `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.

        Example:
        [
            'type=bind',
            'source=lib/modules',
            'destination=/lib/modules',
            'ro=true'
        ]
        becomes
        [
            ...
            'source=/var/lib/ceph/<cluster-fsid>/<daemon-name>/lib/modules',
            ...
        ]
        """
        binds = self.bind_mounts.copy()
        for bind in binds:
            for index, value in enumerate(bind):
                match = re.match(r'^source=(.+)$', value)
                if match:
                    bind[index] = 'source={}'.format(os.path.join(
                        data_dir, match.group(1)))
        return binds

##################################


def touch(file_path: str, uid: Optional[int] = None, gid: Optional[int] = None) -> None:
    Path(file_path).touch()
    if uid and gid:
        os.chown(file_path, uid, gid)


##################################


def dict_get(d: Dict, key: str, default: Any = None, require: bool = False) -> Any:
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

##################################


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

##################################


def get_supported_daemons():
    # type: () -> List[str]
    supported_daemons = list(Ceph.daemons)
    supported_daemons.extend(Monitoring.components)
    supported_daemons.append(NFSGanesha.daemon_type)
    supported_daemons.append(CephIscsi.daemon_type)
    supported_daemons.append(CephNvmeof.daemon_type)
    supported_daemons.append(CustomContainer.daemon_type)
    supported_daemons.append(HAproxy.daemon_type)
    supported_daemons.append(Keepalived.daemon_type)
    supported_daemons.append(CephadmAgent.daemon_type)
    supported_daemons.append(SNMPGateway.daemon_type)
    supported_daemons.extend(Tracing.components)
    assert len(supported_daemons) == len(set(supported_daemons))
    return supported_daemons

##################################


class PortOccupiedError(Error):
    pass


def attempt_bind(ctx, s, address, port):
    # type: (CephadmContext, socket.socket, str, int) -> None
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((address, port))
    except OSError as e:
        if e.errno == errno.EADDRINUSE:
            msg = 'Cannot bind to IP %s port %d: %s' % (address, port, e)
            logger.warning(msg)
            raise PortOccupiedError(msg)
        else:
            raise e
    except Exception as e:
        raise Error(e)
    finally:
        s.close()


def port_in_use(ctx: CephadmContext, endpoint: EndPoint) -> bool:
    """Detect whether a port is in use on the local machine - IPv4 and IPv6"""
    logger.info('Verifying port %s ...' % str(endpoint))

    def _port_in_use(af: socket.AddressFamily, address: str) -> bool:
        try:
            s = socket.socket(af, socket.SOCK_STREAM)
            attempt_bind(ctx, s, address, endpoint.port)
        except PortOccupiedError:
            return True
        except OSError as e:
            if e.errno in (errno.EAFNOSUPPORT, errno.EADDRNOTAVAIL):
                # Ignore EAFNOSUPPORT and EADDRNOTAVAIL as two interfaces are
                # being tested here and one might be intentionally be disabled.
                # In that case no error should be raised.
                return False
            else:
                raise e
        return False

    if endpoint.ip != '0.0.0.0' and endpoint.ip != '::':
        if is_ipv6(endpoint.ip):
            return _port_in_use(socket.AF_INET6, endpoint.ip)
        else:
            return _port_in_use(socket.AF_INET, endpoint.ip)

    return any(_port_in_use(af, address) for af, address in (
        (socket.AF_INET, '0.0.0.0'),
        (socket.AF_INET6, '::')
    ))


def check_ip_port(ctx, ep):
    # type: (CephadmContext, EndPoint) -> None
    if not ctx.skip_ping_check:
        logger.info(f'Verifying IP {ep.ip} port {ep.port} ...')
        if is_ipv6(ep.ip):
            s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            ip = unwrap_ipv6(ep.ip)
        else:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ip = ep.ip
        attempt_bind(ctx, s, ip, ep.port)

##################################


# this is an abbreviated version of
# https://github.com/benediktschmitt/py-filelock/blob/master/filelock.py
# that drops all of the compatibility (this is Unix/Linux only).

class Timeout(TimeoutError):
    """
    Raised when the lock could not be acquired in *timeout*
    seconds.
    """

    def __init__(self, lock_file: str) -> None:
        """
        """
        #: The path of the file lock.
        self.lock_file = lock_file
        return None

    def __str__(self) -> str:
        temp = "The file lock '{}' could not be acquired."\
               .format(self.lock_file)
        return temp


class _Acquire_ReturnProxy(object):
    def __init__(self, lock: 'FileLock') -> None:
        self.lock = lock
        return None

    def __enter__(self) -> 'FileLock':
        return self.lock

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.lock.release()
        return None


class FileLock(object):
    def __init__(self, ctx: CephadmContext, name: str, timeout: int = -1) -> None:
        if not os.path.exists(LOCK_DIR):
            os.mkdir(LOCK_DIR, 0o700)
        self._lock_file = os.path.join(LOCK_DIR, name + '.lock')
        self.ctx = ctx

        # The file descriptor for the *_lock_file* as it is returned by the
        # os.open() function.
        # This file lock is only NOT None, if the object currently holds the
        # lock.
        self._lock_file_fd: Optional[int] = None
        self.timeout = timeout
        # The lock counter is used for implementing the nested locking
        # mechanism. Whenever the lock is acquired, the counter is increased and
        # the lock is only released, when this value is 0 again.
        self._lock_counter = 0
        return None

    @property
    def is_locked(self) -> bool:
        return self._lock_file_fd is not None

    def acquire(self, timeout: Optional[int] = None, poll_intervall: float = 0.05) -> _Acquire_ReturnProxy:
        """
        Acquires the file lock or fails with a :exc:`Timeout` error.
        .. code-block:: python
            # You can use this method in the context manager (recommended)
            with lock.acquire():
                pass
            # Or use an equivalent try-finally construct:
            lock.acquire()
            try:
                pass
            finally:
                lock.release()
        :arg float timeout:
            The maximum time waited for the file lock.
            If ``timeout < 0``, there is no timeout and this method will
            block until the lock could be acquired.
            If ``timeout`` is None, the default :attr:`~timeout` is used.
        :arg float poll_intervall:
            We check once in *poll_intervall* seconds if we can acquire the
            file lock.
        :raises Timeout:
            if the lock could not be acquired in *timeout* seconds.
        .. versionchanged:: 2.0.0
            This method returns now a *proxy* object instead of *self*,
            so that it can be used in a with statement without side effects.
        """

        # Use the default timeout, if no timeout is provided.
        if timeout is None:
            timeout = self.timeout

        # Increment the number right at the beginning.
        # We can still undo it, if something fails.
        self._lock_counter += 1

        lock_id = id(self)
        lock_filename = self._lock_file
        start_time = time.time()
        try:
            while True:
                if not self.is_locked:
                    logger.log(QUIET_LOG_LEVEL, 'Acquiring lock %s on %s', lock_id,
                               lock_filename)
                    self._acquire()

                if self.is_locked:
                    logger.log(QUIET_LOG_LEVEL, 'Lock %s acquired on %s', lock_id,
                               lock_filename)
                    break
                elif timeout >= 0 and time.time() - start_time > timeout:
                    logger.warning('Timeout acquiring lock %s on %s', lock_id,
                                   lock_filename)
                    raise Timeout(self._lock_file)
                else:
                    logger.log(
                        QUIET_LOG_LEVEL,
                        'Lock %s not acquired on %s, waiting %s seconds ...',
                        lock_id, lock_filename, poll_intervall
                    )
                    time.sleep(poll_intervall)
        except Exception:
            # Something did go wrong, so decrement the counter.
            self._lock_counter = max(0, self._lock_counter - 1)

            raise
        return _Acquire_ReturnProxy(lock=self)

    def release(self, force: bool = False) -> None:
        """
        Releases the file lock.
        Please note, that the lock is only completely released, if the lock
        counter is 0.
        Also note, that the lock file itself is not automatically deleted.
        :arg bool force:
            If true, the lock counter is ignored and the lock is released in
            every case.
        """
        if self.is_locked:
            self._lock_counter -= 1

            if self._lock_counter == 0 or force:
                # lock_id = id(self)
                # lock_filename = self._lock_file

                # Can't log in shutdown:
                #  File "/usr/lib64/python3.9/logging/__init__.py", line 1175, in _open
                #    NameError: name 'open' is not defined
                # logger.debug('Releasing lock %s on %s', lock_id, lock_filename)
                self._release()
                self._lock_counter = 0
                # logger.debug('Lock %s released on %s', lock_id, lock_filename)

        return None

    def __enter__(self) -> 'FileLock':
        self.acquire()
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.release()
        return None

    def __del__(self) -> None:
        self.release(force=True)
        return None

    def _acquire(self) -> None:
        open_mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
        fd = os.open(self._lock_file, open_mode)

        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (IOError, OSError):
            os.close(fd)
        else:
            self._lock_file_fd = fd
        return None

    def _release(self) -> None:
        # Do not remove the lockfile:
        #
        #   https://github.com/benediktschmitt/py-filelock/issues/31
        #   https://stackoverflow.com/questions/17708885/flock-removing-locked-file-without-race-condition
        fd = self._lock_file_fd
        self._lock_file_fd = None
        fcntl.flock(fd, fcntl.LOCK_UN)  # type: ignore
        os.close(fd)  # type: ignore
        return None


##################################
# Popen wrappers, lifted from ceph-volume

class CallVerbosity(Enum):
    #####
    # Format:
    # Normal Operation: <log-level-when-no-errors>, Errors: <log-level-when-error>
    #
    # NOTE: QUIET log level is custom level only used when --verbose is passed
    #####

    # Normal Operation: None, Errors: None
    SILENT = 0
    # Normal Operation: QUIET, Error: QUIET
    QUIET = 1
    # Normal Operation: DEBUG, Error: DEBUG
    DEBUG = 2
    # Normal Operation: QUIET, Error: INFO
    QUIET_UNLESS_ERROR = 3
    # Normal Operation: DEBUG, Error: INFO
    VERBOSE_ON_FAILURE = 4
    # Normal Operation: INFO, Error: INFO
    VERBOSE = 5

    def success_log_level(self) -> int:
        _verbosity_level_to_log_level = {
            self.SILENT: 0,
            self.QUIET: QUIET_LOG_LEVEL,
            self.DEBUG: logging.DEBUG,
            self.QUIET_UNLESS_ERROR: QUIET_LOG_LEVEL,
            self.VERBOSE_ON_FAILURE: logging.DEBUG,
            self.VERBOSE: logging.INFO
        }
        return _verbosity_level_to_log_level[self]  # type: ignore

    def error_log_level(self) -> int:
        _verbosity_level_to_log_level = {
            self.SILENT: 0,
            self.QUIET: QUIET_LOG_LEVEL,
            self.DEBUG: logging.DEBUG,
            self.QUIET_UNLESS_ERROR: logging.INFO,
            self.VERBOSE_ON_FAILURE: logging.INFO,
            self.VERBOSE: logging.INFO
        }
        return _verbosity_level_to_log_level[self]  # type: ignore


# disable coverage for the next block. this is copy-n-paste
# from other code for compatibilty on older python versions
if sys.version_info < (3, 8):  # pragma: no cover
    import itertools
    import threading
    import warnings
    from asyncio import events

    class ThreadedChildWatcher(asyncio.AbstractChildWatcher):
        """Threaded child watcher implementation.
        The watcher uses a thread per process
        for waiting for the process finish.
        It doesn't require subscription on POSIX signal
        but a thread creation is not free.
        The watcher has O(1) complexity, its performance doesn't depend
        on amount of spawn processes.
        """

        def __init__(self) -> None:
            self._pid_counter = itertools.count(0)
            self._threads: Dict[Any, Any] = {}

        def is_active(self) -> bool:
            return True

        def close(self) -> None:
            self._join_threads()

        def _join_threads(self) -> None:
            """Internal: Join all non-daemon threads"""
            threads = [thread for thread in list(self._threads.values())
                       if thread.is_alive() and not thread.daemon]
            for thread in threads:
                thread.join()

        def __enter__(self) -> Any:
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            pass

        def __del__(self, _warn: Any = warnings.warn) -> None:
            threads = [thread for thread in list(self._threads.values())
                       if thread.is_alive()]
            if threads:
                _warn(f'{self.__class__} has registered but not finished child processes',
                      ResourceWarning,
                      source=self)

        def add_child_handler(self, pid: Any, callback: Any, *args: Any) -> None:
            loop = events.get_event_loop()
            thread = threading.Thread(target=self._do_waitpid,
                                      name=f'waitpid-{next(self._pid_counter)}',
                                      args=(loop, pid, callback, args),
                                      daemon=True)
            self._threads[pid] = thread
            thread.start()

        def remove_child_handler(self, pid: Any) -> bool:
            # asyncio never calls remove_child_handler() !!!
            # The method is no-op but is implemented because
            # abstract base classe requires it
            return True

        def attach_loop(self, loop: Any) -> None:
            pass

        def _do_waitpid(self, loop: Any, expected_pid: Any, callback: Any, args: Any) -> None:
            assert expected_pid > 0

            try:
                pid, status = os.waitpid(expected_pid, 0)
            except ChildProcessError:
                # The child process is already reaped
                # (may happen if waitpid() is called elsewhere).
                pid = expected_pid
                returncode = 255
                logger.warning(
                    'Unknown child process pid %d, will report returncode 255',
                    pid)
            else:
                if os.WIFEXITED(status):
                    returncode = os.WEXITSTATUS(status)
                elif os.WIFSIGNALED(status):
                    returncode = -os.WTERMSIG(status)
                else:
                    raise ValueError(f'unknown wait status {status}')
                if loop.get_debug():
                    logger.debug('process %s exited with returncode %s',
                                 expected_pid, returncode)

            if loop.is_closed():
                logger.warning('Loop %r that handles pid %r is closed', loop, pid)
            else:
                loop.call_soon_threadsafe(callback, pid, returncode, *args)

            self._threads.pop(expected_pid)

    # unlike SafeChildWatcher which handles SIGCHLD in the main thread,
    # ThreadedChildWatcher runs in a separated thread, hence allows us to
    # run create_subprocess_exec() in non-main thread, see
    # https://bugs.python.org/issue35621
    asyncio.set_child_watcher(ThreadedChildWatcher())


try:
    from asyncio import run as async_run   # type: ignore[attr-defined]
except ImportError:  # pragma: no cover
    # disable coverage for this block. it should be a copy-n-paste from
    # from newer libs for compatibilty on older python versions
    def async_run(coro):  # type: ignore
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            finally:
                asyncio.set_event_loop(None)
                loop.close()


def call(ctx: CephadmContext,
         command: List[str],
         desc: Optional[str] = None,
         verbosity: CallVerbosity = CallVerbosity.VERBOSE_ON_FAILURE,
         timeout: Optional[int] = DEFAULT_TIMEOUT,
         **kwargs: Any) -> Tuple[str, str, int]:
    """
    Wrap subprocess.Popen to

    - log stdout/stderr to a logger,
    - decode utf-8
    - cleanly return out, err, returncode

    :param timeout: timeout in seconds
    """

    prefix = command[0] if desc is None else desc
    if prefix:
        prefix += ': '
    timeout = timeout or ctx.timeout

    async def run_with_timeout() -> Tuple[str, str, int]:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=os.environ.copy())
        assert process.stdout
        assert process.stderr
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout,
            )
        except asyncio.TimeoutError:
            # try to terminate the process assuming it is still running.  It's
            # possible that even after killing the process it will not
            # complete, particularly if it is D-state.  If that happens the
            # process.wait call will block, but we're no worse off than before
            # when the timeout did not work.  Additionally, there are other
            # corner-cases we could try and handle here but we decided to start
            # simple.
            process.kill()
            await process.wait()
            logger.info(prefix + f'timeout after {timeout} seconds')
            return '', '', 124
        else:
            assert process.returncode is not None
            return (
                stdout.decode('utf-8'),
                stderr.decode('utf-8'),
                process.returncode,
            )

    stdout, stderr, returncode = async_run(run_with_timeout())
    log_level = verbosity.success_log_level()
    if returncode != 0:
        log_level = verbosity.error_log_level()
        logger.log(log_level, f'Non-zero exit code {returncode} from {" ".join(command)}')
    for line in stdout.splitlines():
        logger.log(log_level, prefix + 'stdout ' + line)
    for line in stderr.splitlines():
        logger.log(log_level, prefix + 'stderr ' + line)
    return stdout, stderr, returncode


def call_throws(
        ctx: CephadmContext,
        command: List[str],
        desc: Optional[str] = None,
        verbosity: CallVerbosity = CallVerbosity.VERBOSE_ON_FAILURE,
        timeout: Optional[int] = DEFAULT_TIMEOUT,
        **kwargs: Any) -> Tuple[str, str, int]:
    out, err, ret = call(ctx, command, desc, verbosity, timeout, **kwargs)
    if ret:
        for s in (out, err):
            if s.strip() and len(s.splitlines()) <= 2:  # readable message?
                raise RuntimeError(f'Failed command: {" ".join(command)}: {s}')
        raise RuntimeError('Failed command: %s' % ' '.join(command))
    return out, err, ret


def call_timeout(ctx, command, timeout):
    # type: (CephadmContext, List[str], int) -> int
    logger.debug('Running command (timeout=%s): %s'
                 % (timeout, ' '.join(command)))

    def raise_timeout(command, timeout):
        # type: (List[str], int) -> NoReturn
        msg = 'Command `%s` timed out after %s seconds' % (command, timeout)
        logger.debug(msg)
        raise TimeoutExpired(msg)

    try:
        return subprocess.call(command, timeout=timeout, env=os.environ.copy())
    except subprocess.TimeoutExpired:
        raise_timeout(command, timeout)

##################################


def json_loads_retry(cli_func: Callable[[], str]) -> Any:
    for sleep_secs in [1, 4, 4]:
        try:
            return json.loads(cli_func())
        except json.JSONDecodeError:
            logger.debug('Invalid JSON. Retrying in %s seconds...' % sleep_secs)
            time.sleep(sleep_secs)
    return json.loads(cli_func())


def is_available(ctx, what, func):
    # type: (CephadmContext, str, Callable[[], bool]) -> None
    """
    Wait for a service to become available

    :param what: the name of the service
    :param func: the callable object that determines availability
    """
    retry = ctx.retry
    logger.info('Waiting for %s...' % what)
    num = 1
    while True:
        if func():
            logger.info('%s is available'
                        % what)
            break
        elif num > retry:
            raise Error('%s not available after %s tries'
                        % (what, retry))

        logger.info('%s not available, waiting (%s/%s)...'
                    % (what, num, retry))

        num += 1
        time.sleep(2)


def read_config(fn):
    # type: (Optional[str]) -> ConfigParser
    cp = ConfigParser()
    if fn:
        cp.read(fn)
    return cp


def pathify(p):
    # type: (str) -> str
    p = os.path.expanduser(p)
    return os.path.abspath(p)


def get_file_timestamp(fn):
    # type: (str) -> Optional[str]
    try:
        mt = os.path.getmtime(fn)
        return datetime.datetime.fromtimestamp(
            mt, tz=datetime.timezone.utc
        ).strftime(DATEFMT)
    except Exception:
        return None


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
            return datetime.datetime.strptime(s, f).astimezone(tz=datetime.timezone.utc).strftime(DATEFMT)
        except ValueError:
            pass
    return None


def _parse_podman_version(version_str):
    # type: (str) -> Tuple[int, ...]
    def to_int(val: str, org_e: Optional[Exception] = None) -> int:
        if not val and org_e:
            raise org_e
        try:
            return int(val)
        except ValueError as e:
            return to_int(val[0:-1], org_e or e)

    return tuple(map(to_int, version_str.split('.')))


def get_hostname():
    # type: () -> str
    return socket.gethostname()


def get_short_hostname():
    # type: () -> str
    return get_hostname().split('.', 1)[0]


def get_fqdn():
    # type: () -> str
    return socket.getfqdn() or socket.gethostname()


def get_ip_addresses(hostname: str) -> Tuple[List[str], List[str]]:
    items = socket.getaddrinfo(hostname, None,
                               flags=socket.AI_CANONNAME,
                               type=socket.SOCK_STREAM)
    ipv4_addresses = [i[4][0] for i in items if i[0] == socket.AF_INET]
    ipv6_addresses = [i[4][0] for i in items if i[0] == socket.AF_INET6]
    return ipv4_addresses, ipv6_addresses


def get_arch():
    # type: () -> str
    return platform.uname().machine


def generate_service_id():
    # type: () -> str
    return get_short_hostname() + '.' + ''.join(random.choice(string.ascii_lowercase)
                                                for _ in range(6))


def generate_password():
    # type: () -> str
    return ''.join(random.choice(string.ascii_lowercase + string.digits)
                   for i in range(10))


def normalize_container_id(i):
    # type: (str) -> str
    # docker adds the sha256: prefix, but AFAICS both
    # docker (18.09.7 in bionic at least) and podman
    # both always use sha256, so leave off the prefix
    # for consistency.
    prefix = 'sha256:'
    if i.startswith(prefix):
        i = i[len(prefix):]
    return i


def make_fsid():
    # type: () -> str
    return str(uuid.uuid1())


def is_fsid(s):
    # type: (str) -> bool
    try:
        uuid.UUID(s)
    except ValueError:
        return False
    return True


def validate_fsid(func: FuncT) -> FuncT:
    @wraps(func)
    def _validate_fsid(ctx: CephadmContext) -> Any:
        if 'fsid' in ctx and ctx.fsid:
            if not is_fsid(ctx.fsid):
                raise Error('not an fsid: %s' % ctx.fsid)
        return func(ctx)
    return cast(FuncT, _validate_fsid)


def infer_fsid(func: FuncT) -> FuncT:
    """
    If we only find a single fsid in /var/lib/ceph/*, use that
    """
    @infer_config
    @wraps(func)
    def _infer_fsid(ctx: CephadmContext) -> Any:
        if 'fsid' in ctx and ctx.fsid:
            logger.debug('Using specified fsid: %s' % ctx.fsid)
            return func(ctx)

        fsids = set()

        cp = read_config(ctx.config)
        if cp.has_option('global', 'fsid'):
            fsids.add(cp.get('global', 'fsid'))

        daemon_list = list_daemons(ctx, detail=False)
        for daemon in daemon_list:
            if not is_fsid(daemon['fsid']):
                # 'unknown' fsid
                continue
            elif 'name' not in ctx or not ctx.name:
                # ctx.name not specified
                fsids.add(daemon['fsid'])
            elif daemon['name'] == ctx.name:
                # ctx.name is a match
                fsids.add(daemon['fsid'])
        fsids = sorted(fsids)

        if not fsids:
            # some commands do not always require an fsid
            pass
        elif len(fsids) == 1:
            logger.info('Inferring fsid %s' % fsids[0])
            ctx.fsid = fsids[0]
        else:
            raise Error('Cannot infer an fsid, one must be specified (using --fsid): %s' % fsids)
        return func(ctx)

    return cast(FuncT, _infer_fsid)


def infer_config(func: FuncT) -> FuncT:
    """
    Infer the cluster configuration using the following priority order:
     1- if the user has provided custom conf file (-c option) use it
     2- otherwise if daemon --name has been provided use daemon conf
     3- otherwise find the mon daemon conf file and use it (if v1)
     4- otherwise if {ctx.data_dir}/{fsid}/{CEPH_CONF_DIR} dir exists use it
     5- finally: fallback to the default file /etc/ceph/ceph.conf
    """
    @wraps(func)
    def _infer_config(ctx: CephadmContext) -> Any:

        def config_path(daemon_type: str, daemon_name: str) -> str:
            data_dir = get_data_dir(ctx.fsid, ctx.data_dir, daemon_type, daemon_name)
            return os.path.join(data_dir, 'config')

        def get_mon_daemon_name(fsid: str) -> Optional[str]:
            daemon_list = list_daemons(ctx, detail=False)
            for daemon in daemon_list:
                if (
                    daemon.get('name', '').startswith('mon.')
                    and daemon.get('fsid', '') == fsid
                    and daemon.get('style', '') == 'cephadm:v1'
                    and os.path.exists(config_path('mon', daemon['name'].split('.', 1)[1]))
                ):
                    return daemon['name']
            return None

        ctx.config = ctx.config if 'config' in ctx else None
        #  check if user has provided conf by using -c option
        if ctx.config and (ctx.config != CEPH_DEFAULT_CONF):
            logger.debug(f'Using specified config: {ctx.config}')
            return func(ctx)

        if 'fsid' in ctx and ctx.fsid:
            name = ctx.name if ('name' in ctx and ctx.name) else get_mon_daemon_name(ctx.fsid)
            if name is not None:
                # daemon name has been specified (or inferred from mon), let's use its conf
                ctx.config = config_path(name.split('.', 1)[0], name.split('.', 1)[1])
            else:
                # no daemon, in case the cluster has a config dir then use it
                ceph_conf = f'{ctx.data_dir}/{ctx.fsid}/{CEPH_CONF_DIR}/{CEPH_CONF}'
                if os.path.exists(ceph_conf):
                    ctx.config = ceph_conf

        if ctx.config:
            logger.info(f'Inferring config {ctx.config}')
        elif os.path.exists(CEPH_DEFAULT_CONF):
            logger.debug(f'Using default config {CEPH_DEFAULT_CONF}')
            ctx.config = CEPH_DEFAULT_CONF
        return func(ctx)

    return cast(FuncT, _infer_config)


def _get_default_image(ctx: CephadmContext) -> str:
    if DEFAULT_IMAGE_IS_MAIN:
        warn = """This is a development version of cephadm.
For information regarding the latest stable release:
    https://docs.ceph.com/docs/{}/cephadm/install
""".format(LATEST_STABLE_RELEASE)
        for line in warn.splitlines():
            logger.warning('{}{}{}'.format(termcolor.yellow, line, termcolor.end))
    return DEFAULT_IMAGE


def infer_image(func: FuncT) -> FuncT:
    """
    Use the most recent ceph image
    """
    @wraps(func)
    def _infer_image(ctx: CephadmContext) -> Any:
        if not ctx.image:
            ctx.image = os.environ.get('CEPHADM_IMAGE')
        if not ctx.image:
            ctx.image = infer_local_ceph_image(ctx, ctx.container_engine.path)
        if not ctx.image:
            ctx.image = _get_default_image(ctx)
        return func(ctx)

    return cast(FuncT, _infer_image)


def require_image(func: FuncT) -> FuncT:
    """
    Require the global --image flag to be set
    """
    @wraps(func)
    def _require_image(ctx: CephadmContext) -> Any:
        if not ctx.image:
            raise Error('This command requires the global --image option to be set')
        return func(ctx)

    return cast(FuncT, _require_image)


def default_image(func: FuncT) -> FuncT:
    @wraps(func)
    def _default_image(ctx: CephadmContext) -> Any:
        update_default_image(ctx)
        return func(ctx)

    return cast(FuncT, _default_image)


def update_default_image(ctx: CephadmContext) -> None:
    if getattr(ctx, 'image', None):
        return
    ctx.image = None  # ensure ctx.image exists to avoid repeated `getattr`s
    name = getattr(ctx, 'name', None)
    if name:
        type_ = name.split('.', 1)[0]
        if type_ in Monitoring.components:
            ctx.image = Monitoring.components[type_]['image']
        if type_ == 'haproxy':
            ctx.image = HAproxy.default_image
        if type_ == 'keepalived':
            ctx.image = Keepalived.default_image
        if type_ == SNMPGateway.daemon_type:
            ctx.image = SNMPGateway.default_image
        if type_ == CephNvmeof.daemon_type:
            ctx.image = CephNvmeof.default_image
        if type_ in Tracing.components:
            ctx.image = Tracing.components[type_]['image']
    if not ctx.image:
        ctx.image = os.environ.get('CEPHADM_IMAGE')
    if not ctx.image:
        ctx.image = _get_default_image(ctx)


def executes_early(func: FuncT) -> FuncT:
    """Decorator that indicates the command function is meant to have no
    dependencies and no environmental requirements and can therefore be
    executed as non-root and with no logging, etc. Commands that have this
    decorator applied must be simple and self-contained.
    """
    cast(Any, func)._execute_early = True
    return func


def deprecated_command(func: FuncT) -> FuncT:
    @wraps(func)
    def _deprecated_command(ctx: CephadmContext) -> Any:
        logger.warning(f'Deprecated command used: {func}')
        if NO_DEPRECATED:
            raise Error('running deprecated commands disabled')
        return func(ctx)

    return cast(FuncT, _deprecated_command)


def get_container_info(ctx: CephadmContext, daemon_filter: str, by_name: bool) -> Optional[ContainerInfo]:
    """
    :param ctx: Cephadm context
    :param daemon_filter: daemon name or type
    :param by_name: must be set to True if daemon name is provided
    :return: Container information or None
    """
    def daemon_name_or_type(daemon: Dict[str, str]) -> str:
        return daemon['name'] if by_name else daemon['name'].split('.', 1)[0]

    if by_name and '.' not in daemon_filter:
        logger.warning(f'Trying to get container info using invalid daemon name {daemon_filter}')
        return None
    daemons = list_daemons(ctx, detail=False)
    matching_daemons = [d for d in daemons if daemon_name_or_type(d) == daemon_filter and d['fsid'] == ctx.fsid]
    if matching_daemons:
        d_type, d_id = matching_daemons[0]['name'].split('.', 1)
        out, _, code = get_container_stats(ctx, ctx.container_engine.path, ctx.fsid, d_type, d_id)
        if not code:
            (container_id, image_name, image_id, start, version) = out.strip().split(',')
            return ContainerInfo(container_id, image_name, image_id, start, version)
    return None


def infer_local_ceph_image(ctx: CephadmContext, container_path: str) -> Optional[str]:
    """
     Infer the local ceph image based on the following priority criteria:
       1- the image specified by --image arg (if provided).
       2- the same image as the daemon container specified by --name arg (if provided).
       3- image used by any ceph container running on the host. In this case we use daemon types.
       4- if no container is found then we use the most ceph recent image on the host.

     Note: any selected container must have the same fsid inferred previously.

    :return: The most recent local ceph image (already pulled)
    """
    # '|' special character is used to separate the output fields into:
    #  - Repository@digest
    #  - Image Id
    #  - Image Tag
    #  - Image creation date
    out, _, _ = call_throws(ctx,
                            [container_path, 'images',
                             '--filter', 'label=ceph=True',
                             '--filter', 'dangling=false',
                             '--format', '{{.Repository}}@{{.Digest}}|{{.ID}}|{{.Tag}}|{{.CreatedAt}}'])

    container_info = None
    daemon_name = ctx.name if ('name' in ctx and ctx.name and '.' in ctx.name) else None
    daemons_ls = [daemon_name] if daemon_name is not None else Ceph.daemons  # daemon types: 'mon', 'mgr', etc
    for daemon in daemons_ls:
        container_info = get_container_info(ctx, daemon, daemon_name is not None)
        if container_info is not None:
            logger.debug(f"Using container info for daemon '{daemon}'")
            break

    for image in out.splitlines():
        if image and not image.isspace():
            (digest, image_id, tag, created_date) = image.lstrip().split('|')
            if container_info is not None and image_id not in container_info.image_id:
                continue
            if digest and not digest.endswith('@'):
                logger.info(f"Using ceph image with id '{image_id}' and tag '{tag}' created on {created_date}\n{digest}")
                return digest
    return None


def write_tmp(s, uid, gid):
    # type: (str, int, int) -> IO[str]
    tmp_f = tempfile.NamedTemporaryFile(mode='w',
                                        prefix='ceph-tmp')
    os.fchown(tmp_f.fileno(), uid, gid)
    tmp_f.write(s)
    tmp_f.flush()

    return tmp_f


def makedirs(dir, uid, gid, mode):
    # type: (str, int, int, int) -> None
    if not os.path.exists(dir):
        os.makedirs(dir, mode=mode)
    else:
        os.chmod(dir, mode)
    os.chown(dir, uid, gid)
    os.chmod(dir, mode)   # the above is masked by umask...


def get_data_dir(fsid, data_dir, t, n):
    # type: (str, str, str, Union[int, str]) -> str
    return os.path.join(data_dir, fsid, '%s.%s' % (t, n))


def get_log_dir(fsid, log_dir):
    # type: (str, str) -> str
    return os.path.join(log_dir, fsid)


def make_data_dir_base(fsid, data_dir, uid, gid):
    # type: (str, str, int, int) -> str
    data_dir_base = os.path.join(data_dir, fsid)
    makedirs(data_dir_base, uid, gid, DATA_DIR_MODE)
    makedirs(os.path.join(data_dir_base, 'crash'), uid, gid, DATA_DIR_MODE)
    makedirs(os.path.join(data_dir_base, 'crash', 'posted'), uid, gid,
             DATA_DIR_MODE)
    return data_dir_base


def make_data_dir(ctx, fsid, daemon_type, daemon_id, uid=None, gid=None):
    # type: (CephadmContext, str, str, Union[int, str], Optional[int], Optional[int]) -> str
    if uid is None or gid is None:
        uid, gid = extract_uid_gid(ctx)
    make_data_dir_base(fsid, ctx.data_dir, uid, gid)
    data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
    makedirs(data_dir, uid, gid, DATA_DIR_MODE)
    return data_dir


def make_log_dir(ctx, fsid, uid=None, gid=None):
    # type: (CephadmContext, str, Optional[int], Optional[int]) -> str
    if uid is None or gid is None:
        uid, gid = extract_uid_gid(ctx)
    log_dir = get_log_dir(fsid, ctx.log_dir)
    makedirs(log_dir, uid, gid, LOG_DIR_MODE)
    return log_dir


def make_var_run(ctx, fsid, uid, gid):
    # type: (CephadmContext, str, int, int) -> None
    call_throws(ctx, ['install', '-d', '-m0770', '-o', str(uid), '-g', str(gid),
                      '/var/run/ceph/%s' % fsid])


def copy_tree(ctx, src, dst, uid=None, gid=None):
    # type: (CephadmContext, List[str], str, Optional[int], Optional[int]) -> None
    """
    Copy a directory tree from src to dst
    """
    if uid is None or gid is None:
        (uid, gid) = extract_uid_gid(ctx)

    for src_dir in src:
        dst_dir = dst
        if os.path.isdir(dst):
            dst_dir = os.path.join(dst, os.path.basename(src_dir))

        logger.debug('copy directory `%s` -> `%s`' % (src_dir, dst_dir))
        shutil.rmtree(dst_dir, ignore_errors=True)
        shutil.copytree(src_dir, dst_dir)  # dirs_exist_ok needs python 3.8

        for dirpath, dirnames, filenames in os.walk(dst_dir):
            logger.debug('chown %s:%s `%s`' % (uid, gid, dirpath))
            os.chown(dirpath, uid, gid)
            for filename in filenames:
                logger.debug('chown %s:%s `%s`' % (uid, gid, filename))
                os.chown(os.path.join(dirpath, filename), uid, gid)


def copy_files(ctx, src, dst, uid=None, gid=None):
    # type: (CephadmContext, List[str], str, Optional[int], Optional[int]) -> None
    """
    Copy a files from src to dst
    """
    if uid is None or gid is None:
        (uid, gid) = extract_uid_gid(ctx)

    for src_file in src:
        dst_file = dst
        if os.path.isdir(dst):
            dst_file = os.path.join(dst, os.path.basename(src_file))

        logger.debug('copy file `%s` -> `%s`' % (src_file, dst_file))
        shutil.copyfile(src_file, dst_file)

        logger.debug('chown %s:%s `%s`' % (uid, gid, dst_file))
        os.chown(dst_file, uid, gid)


def move_files(ctx, src, dst, uid=None, gid=None):
    # type: (CephadmContext, List[str], str, Optional[int], Optional[int]) -> None
    """
    Move files from src to dst
    """
    if uid is None or gid is None:
        (uid, gid) = extract_uid_gid(ctx)

    for src_file in src:
        dst_file = dst
        if os.path.isdir(dst):
            dst_file = os.path.join(dst, os.path.basename(src_file))

        if os.path.islink(src_file):
            # shutil.move() in py2 does not handle symlinks correctly
            src_rl = os.readlink(src_file)
            logger.debug("symlink '%s' -> '%s'" % (dst_file, src_rl))
            os.symlink(src_rl, dst_file)
            os.unlink(src_file)
        else:
            logger.debug("move file '%s' -> '%s'" % (src_file, dst_file))
            shutil.move(src_file, dst_file)
            logger.debug('chown %s:%s `%s`' % (uid, gid, dst_file))
            os.chown(dst_file, uid, gid)


def recursive_chown(path: str, uid: int, gid: int) -> None:
    for dirpath, dirnames, filenames in os.walk(path):
        os.chown(dirpath, uid, gid)
        for filename in filenames:
            os.chown(os.path.join(dirpath, filename), uid, gid)


# copied from distutils
def find_executable(executable: str, path: Optional[str] = None) -> Optional[str]:
    """Tries to find 'executable' in the directories listed in 'path'.
    A string listing directories separated by 'os.pathsep'; defaults to
    os.environ['PATH'].  Returns the complete filename or None if not found.
    """
    _, ext = os.path.splitext(executable)
    if (sys.platform == 'win32') and (ext != '.exe'):
        executable = executable + '.exe'  # pragma: no cover

    if os.path.isfile(executable):
        return executable

    if path is None:
        path = os.environ.get('PATH', None)
        if path is None:
            try:
                path = os.confstr('CS_PATH')
            except (AttributeError, ValueError):
                # os.confstr() or CS_PATH is not available
                path = os.defpath
        # bpo-35755: Don't use os.defpath if the PATH environment variable is
        # set to an empty string

    # PATH='' doesn't match, whereas PATH=':' looks in the current directory
    if not path:
        return None

    paths = path.split(os.pathsep)
    for p in paths:
        f = os.path.join(p, executable)
        if os.path.isfile(f):
            # the file exists, we have a shot at spawn working
            return f
    return None


def find_program(filename):
    # type: (str) -> str
    name = find_executable(filename)
    if name is None:
        raise ValueError('%s not found' % filename)
    return name


def find_container_engine(ctx: CephadmContext) -> Optional[ContainerEngine]:
    if ctx.docker:
        return Docker()
    else:
        for i in CONTAINER_PREFERENCE:
            try:
                return i()
            except Exception:
                pass
    return None


def check_container_engine(ctx: CephadmContext) -> ContainerEngine:
    engine = ctx.container_engine
    if not isinstance(engine, CONTAINER_PREFERENCE):
        # See https://github.com/python/mypy/issues/8993
        exes: List[str] = [i.EXE for i in CONTAINER_PREFERENCE]  # type: ignore
        raise Error('No container engine binary found ({}). Try run `apt/dnf/yum/zypper install <container engine>`'.format(' or '.join(exes)))
    elif isinstance(engine, Podman):
        engine.get_version(ctx)
        if engine.version < MIN_PODMAN_VERSION:
            raise Error('podman version %d.%d.%d or later is required' % MIN_PODMAN_VERSION)
    return engine


def get_unit_name(fsid, daemon_type, daemon_id=None):
    # type: (str, str, Optional[Union[int, str]]) -> str
    # accept either name or type + id
    if daemon_id is not None:
        return 'ceph-%s@%s.%s' % (fsid, daemon_type, daemon_id)
    else:
        return 'ceph-%s@%s' % (fsid, daemon_type)


def get_unit_name_by_daemon_name(ctx: CephadmContext, fsid: str, name: str) -> str:
    daemon = get_daemon_description(ctx, fsid, name)
    try:
        return daemon['systemd_unit']
    except KeyError:
        raise Error('Failed to get unit name for {}'.format(daemon))


def check_unit(ctx, unit_name):
    # type: (CephadmContext, str) -> Tuple[bool, str, bool]
    # NOTE: we ignore the exit code here because systemctl outputs
    # various exit codes based on the state of the service, but the
    # string result is more explicit (and sufficient).
    enabled = False
    installed = False
    try:
        out, err, code = call(ctx, ['systemctl', 'is-enabled', unit_name],
                              verbosity=CallVerbosity.QUIET)
        if code == 0:
            enabled = True
            installed = True
        elif 'disabled' in out:
            installed = True
    except Exception as e:
        logger.warning('unable to run systemctl: %s' % e)
        enabled = False
        installed = False

    state = 'unknown'
    try:
        out, err, code = call(ctx, ['systemctl', 'is-active', unit_name],
                              verbosity=CallVerbosity.QUIET)
        out = out.strip()
        if out in ['active']:
            state = 'running'
        elif out in ['inactive']:
            state = 'stopped'
        elif out in ['failed', 'auto-restart']:
            state = 'error'
        else:
            state = 'unknown'
    except Exception as e:
        logger.warning('unable to run systemctl: %s' % e)
        state = 'unknown'
    return (enabled, state, installed)


def check_units(ctx, units, enabler=None):
    # type: (CephadmContext, List[str], Optional[Packager]) -> bool
    for u in units:
        (enabled, state, installed) = check_unit(ctx, u)
        if enabled and state == 'running':
            logger.info('Unit %s is enabled and running' % u)
            return True
        if enabler is not None:
            if installed:
                logger.info('Enabling unit %s' % u)
                enabler.enable_service(u)
    return False


def is_container_running(ctx: CephadmContext, c: 'CephContainer') -> bool:
    if ctx.name.split('.', 1)[0] in ['agent', 'cephadm-exporter']:
        # these are non-containerized daemon types
        return False
    return bool(get_running_container_name(ctx, c))


def get_running_container_name(ctx: CephadmContext, c: 'CephContainer') -> Optional[str]:
    for name in [c.cname, c.old_cname]:
        out, err, ret = call(ctx, [
            ctx.container_engine.path, 'container', 'inspect',
            '--format', '{{.State.Status}}', name
        ])
        if out.strip() == 'running':
            return name
    return None


def get_legacy_config_fsid(cluster, legacy_dir=None):
    # type: (str, Optional[str]) -> Optional[str]
    config_file = '/etc/ceph/%s.conf' % cluster
    if legacy_dir is not None:
        config_file = os.path.abspath(legacy_dir + config_file)

    if os.path.exists(config_file):
        config = read_config(config_file)
        if config.has_section('global') and config.has_option('global', 'fsid'):
            return config.get('global', 'fsid')
    return None


def get_legacy_daemon_fsid(ctx, cluster,
                           daemon_type, daemon_id, legacy_dir=None):
    # type: (CephadmContext, str, str, Union[int, str], Optional[str]) -> Optional[str]
    fsid = None
    if daemon_type == 'osd':
        try:
            fsid_file = os.path.join(ctx.data_dir,
                                     daemon_type,
                                     'ceph-%s' % daemon_id,
                                     'ceph_fsid')
            if legacy_dir is not None:
                fsid_file = os.path.abspath(legacy_dir + fsid_file)
            with open(fsid_file, 'r') as f:
                fsid = f.read().strip()
        except IOError:
            pass
    if not fsid:
        fsid = get_legacy_config_fsid(cluster, legacy_dir=legacy_dir)
    return fsid


def should_log_to_journald(ctx: CephadmContext) -> bool:
    if ctx.log_to_journald is not None:
        return ctx.log_to_journald
    return isinstance(ctx.container_engine, Podman) and \
        ctx.container_engine.version >= CGROUPS_SPLIT_PODMAN_VERSION


def get_daemon_args(ctx, fsid, daemon_type, daemon_id):
    # type: (CephadmContext, str, str, Union[int, str]) -> List[str]
    r = list()  # type: List[str]

    if daemon_type in Ceph.daemons and daemon_type not in ['crash', 'ceph-exporter']:
        r += [
            '--setuser', 'ceph',
            '--setgroup', 'ceph',
            '--default-log-to-file=false',
        ]
        log_to_journald = should_log_to_journald(ctx)
        if log_to_journald:
            r += [
                '--default-log-to-journald=true',
                '--default-log-to-stderr=false',
            ]
        else:
            r += [
                '--default-log-to-stderr=true',
                '--default-log-stderr-prefix=debug ',
            ]
        if daemon_type == 'mon':
            r += [
                '--default-mon-cluster-log-to-file=false',
            ]
            if log_to_journald:
                r += [
                    '--default-mon-cluster-log-to-journald=true',
                    '--default-mon-cluster-log-to-stderr=false',
                ]
            else:
                r += ['--default-mon-cluster-log-to-stderr=true']
    elif daemon_type in Monitoring.components:
        metadata = Monitoring.components[daemon_type]
        r += metadata.get('args', list())
        # set ip and port to bind to for nodeexporter,alertmanager,prometheus
        if daemon_type not in ['grafana', 'loki', 'promtail']:
            ip = ''
            port = Monitoring.port_map[daemon_type][0]
            meta = fetch_meta(ctx)
            if meta:
                if 'ip' in meta and meta['ip']:
                    ip = meta['ip']
                if 'ports' in meta and meta['ports']:
                    port = meta['ports'][0]
            r += [f'--web.listen-address={ip}:{port}']
            if daemon_type == 'prometheus':
                config = fetch_configs(ctx)
                retention_time = config.get('retention_time', '15d')
                retention_size = config.get('retention_size', '0')  # default to disabled
                r += [f'--storage.tsdb.retention.time={retention_time}']
                r += [f'--storage.tsdb.retention.size={retention_size}']
                scheme = 'http'
                host = get_fqdn()
                # in case host is not an fqdn then we use the IP to
                # avoid producing a broken web.external-url link
                if '.' not in host:
                    ipv4_addrs, ipv6_addrs = get_ip_addresses(get_hostname())
                    # use the first ipv4 (if any) otherwise use the first ipv6
                    addr = next(iter(ipv4_addrs or ipv6_addrs), None)
                    host = wrap_ipv6(addr) if addr else host
                r += [f'--web.external-url={scheme}://{host}:{port}']
        if daemon_type == 'alertmanager':
            config = fetch_configs(ctx)
            peers = config.get('peers', list())  # type: ignore
            for peer in peers:
                r += ['--cluster.peer={}'.format(peer)]
            try:
                r += [f'--web.config.file={config["web_config"]}']
            except KeyError:
                pass
            # some alertmanager, by default, look elsewhere for a config
            r += ['--config.file=/etc/alertmanager/alertmanager.yml']
        if daemon_type == 'promtail':
            r += ['--config.expand-env']
        if daemon_type == 'prometheus':
            config = fetch_configs(ctx)
            try:
                r += [f'--web.config.file={config["web_config"]}']
            except KeyError:
                pass
        if daemon_type == 'node-exporter':
            config = fetch_configs(ctx)
            try:
                r += [f'--web.config.file={config["web_config"]}']
            except KeyError:
                pass
            r += ['--path.procfs=/host/proc',
                  '--path.sysfs=/host/sys',
                  '--path.rootfs=/rootfs']
    elif daemon_type == 'jaeger-agent':
        r.extend(Tracing.components[daemon_type]['daemon_args'])
    elif daemon_type == NFSGanesha.daemon_type:
        nfs_ganesha = NFSGanesha.init(ctx, fsid, daemon_id)
        r += nfs_ganesha.get_daemon_args()
    elif daemon_type == CephExporter.daemon_type:
        ceph_exporter = CephExporter.init(ctx, fsid, daemon_id)
        r.extend(ceph_exporter.get_daemon_args())
    elif daemon_type == HAproxy.daemon_type:
        haproxy = HAproxy.init(ctx, fsid, daemon_id)
        r += haproxy.get_daemon_args()
    elif daemon_type == CustomContainer.daemon_type:
        cc = CustomContainer.init(ctx, fsid, daemon_id)
        r.extend(cc.get_daemon_args())
    elif daemon_type == SNMPGateway.daemon_type:
        sc = SNMPGateway.init(ctx, fsid, daemon_id)
        r.extend(sc.get_daemon_args())

    return r


def create_daemon_dirs(ctx, fsid, daemon_type, daemon_id, uid, gid,
                       config=None, keyring=None):
    # type: (CephadmContext, str, str, Union[int, str], int, int, Optional[str], Optional[str]) ->  None
    data_dir = make_data_dir(ctx, fsid, daemon_type, daemon_id, uid=uid, gid=gid)

    if daemon_type in Ceph.daemons:
        make_log_dir(ctx, fsid, uid=uid, gid=gid)

    if config:
        config_path = os.path.join(data_dir, 'config')
        with write_new(config_path, owner=(uid, gid)) as f:
            f.write(config)

    if keyring:
        keyring_path = os.path.join(data_dir, 'keyring')
        with write_new(keyring_path, owner=(uid, gid)) as f:
            f.write(keyring)

    if daemon_type in Monitoring.components.keys():
        config_json = fetch_configs(ctx)

        # Set up directories specific to the monitoring component
        config_dir = ''
        data_dir_root = ''
        if daemon_type == 'prometheus':
            data_dir_root = get_data_dir(fsid, ctx.data_dir,
                                         daemon_type, daemon_id)
            config_dir = 'etc/prometheus'
            makedirs(os.path.join(data_dir_root, config_dir), uid, gid, 0o755)
            makedirs(os.path.join(data_dir_root, config_dir, 'alerting'), uid, gid, 0o755)
            makedirs(os.path.join(data_dir_root, 'data'), uid, gid, 0o755)
            recursive_chown(os.path.join(data_dir_root, 'etc'), uid, gid)
            recursive_chown(os.path.join(data_dir_root, 'data'), uid, gid)
        elif daemon_type == 'grafana':
            data_dir_root = get_data_dir(fsid, ctx.data_dir,
                                         daemon_type, daemon_id)
            config_dir = 'etc/grafana'
            makedirs(os.path.join(data_dir_root, config_dir), uid, gid, 0o755)
            makedirs(os.path.join(data_dir_root, config_dir, 'certs'), uid, gid, 0o755)
            makedirs(os.path.join(data_dir_root, config_dir, 'provisioning/datasources'), uid, gid, 0o755)
            makedirs(os.path.join(data_dir_root, 'data'), uid, gid, 0o755)
            touch(os.path.join(data_dir_root, 'data', 'grafana.db'), uid, gid)
        elif daemon_type == 'alertmanager':
            data_dir_root = get_data_dir(fsid, ctx.data_dir,
                                         daemon_type, daemon_id)
            config_dir = 'etc/alertmanager'
            makedirs(os.path.join(data_dir_root, config_dir), uid, gid, 0o755)
            makedirs(os.path.join(data_dir_root, config_dir, 'data'), uid, gid, 0o755)
        elif daemon_type == 'promtail':
            data_dir_root = get_data_dir(fsid, ctx.data_dir,
                                         daemon_type, daemon_id)
            config_dir = 'etc/promtail'
            makedirs(os.path.join(data_dir_root, config_dir), uid, gid, 0o755)
            makedirs(os.path.join(data_dir_root, 'data'), uid, gid, 0o755)
        elif daemon_type == 'loki':
            data_dir_root = get_data_dir(fsid, ctx.data_dir,
                                         daemon_type, daemon_id)
            config_dir = 'etc/loki'
            makedirs(os.path.join(data_dir_root, config_dir), uid, gid, 0o755)
            makedirs(os.path.join(data_dir_root, 'data'), uid, gid, 0o755)
        elif daemon_type == 'node-exporter':
            data_dir_root = get_data_dir(fsid, ctx.data_dir,
                                         daemon_type, daemon_id)
            config_dir = 'etc/node-exporter'
            makedirs(os.path.join(data_dir_root, config_dir), uid, gid, 0o755)
            recursive_chown(os.path.join(data_dir_root, 'etc'), uid, gid)

        # populate the config directory for the component from the config-json
        if 'files' in config_json:
            for fname in config_json['files']:
                # work around mypy wierdness where it thinks `str`s aren't Anys
                # when used for dictionary values! feels like possibly a mypy bug?!
                cfg = cast(Dict[str, Any], config_json['files'])
                content = dict_get_join(cfg, fname)
                if os.path.isabs(fname):
                    fpath = os.path.join(data_dir_root, fname.lstrip(os.path.sep))
                else:
                    fpath = os.path.join(data_dir_root, config_dir, fname)
                with write_new(fpath, owner=(uid, gid), encoding='utf-8') as f:
                    f.write(content)

    elif daemon_type == NFSGanesha.daemon_type:
        nfs_ganesha = NFSGanesha.init(ctx, fsid, daemon_id)
        nfs_ganesha.create_daemon_dirs(data_dir, uid, gid)

    elif daemon_type == CephIscsi.daemon_type:
        ceph_iscsi = CephIscsi.init(ctx, fsid, daemon_id)
        ceph_iscsi.create_daemon_dirs(data_dir, uid, gid)

    elif daemon_type == CephNvmeof.daemon_type:
        ceph_nvmeof = CephNvmeof.init(ctx, fsid, daemon_id)
        ceph_nvmeof.create_daemon_dirs(data_dir, uid, gid)

    elif daemon_type == HAproxy.daemon_type:
        haproxy = HAproxy.init(ctx, fsid, daemon_id)
        haproxy.create_daemon_dirs(data_dir, uid, gid)

    elif daemon_type == Keepalived.daemon_type:
        keepalived = Keepalived.init(ctx, fsid, daemon_id)
        keepalived.create_daemon_dirs(data_dir, uid, gid)

    elif daemon_type == CustomContainer.daemon_type:
        cc = CustomContainer.init(ctx, fsid, daemon_id)
        cc.create_daemon_dirs(data_dir, uid, gid)

    elif daemon_type == SNMPGateway.daemon_type:
        sg = SNMPGateway.init(ctx, fsid, daemon_id)
        sg.create_daemon_conf()

    _write_custom_conf_files(ctx, daemon_type, str(daemon_id), fsid, uid, gid)


def _write_custom_conf_files(ctx: CephadmContext, daemon_type: str, daemon_id: str, fsid: str, uid: int, gid: int) -> None:
    # mostly making this its own function to make unit testing easier
    ccfiles = fetch_custom_config_files(ctx)
    if not ccfiles:
        return
    custom_config_dir = os.path.join(ctx.data_dir, fsid, 'custom_config_files', f'{daemon_type}.{daemon_id}')
    if not os.path.exists(custom_config_dir):
        makedirs(custom_config_dir, uid, gid, 0o755)
    mandatory_keys = ['mount_path', 'content']
    for ccf in ccfiles:
        if all(k in ccf for k in mandatory_keys):
            file_path = os.path.join(custom_config_dir, os.path.basename(ccf['mount_path']))
            with write_new(file_path, owner=(uid, gid), encoding='utf-8') as f:
                f.write(ccf['content'])
            # temporary workaround to make custom config files work for tcmu-runner
            # container we deploy with iscsi until iscsi is refactored
            if daemon_type == 'iscsi':
                tcmu_config_dir = custom_config_dir + '.tcmu'
                if not os.path.exists(tcmu_config_dir):
                    makedirs(tcmu_config_dir, uid, gid, 0o755)
                tcmu_file_path = os.path.join(tcmu_config_dir, os.path.basename(ccf['mount_path']))
                with write_new(tcmu_file_path, owner=(uid, gid), encoding='utf-8') as f:
                    f.write(ccf['content'])


def get_parm(option: str) -> Dict[str, str]:
    js = _get_config_json(option)
    # custom_config_files is a special field that may be in the config
    # dict. It is used for mounting custom config files into daemon's containers
    # and should be accessed through the "fetch_custom_config_files" function.
    # For get_parm we need to discard it.
    js.pop('custom_config_files', None)
    return js


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


def fetch_meta(ctx: CephadmContext) -> Dict[str, Any]:
    """Return a dict containing metadata about a deployment.
    """
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


def fetch_tcp_ports(ctx: CephadmContext) -> List[EndPoint]:
    """Return a list of Endpoints, which have a port and ip attribute
    """
    ports = getattr(ctx, 'tcp_ports', None)
    if ports is None:
        ports = []
    if isinstance(ports, str):
        ports = list(map(int, ports.split()))
    port_ips: Dict[str, str] = {}
    port_ips_attr: Union[str, Dict[str, str], None] = getattr(ctx, 'port_ips', None)
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


def get_container_binds(ctx, fsid, daemon_type, daemon_id):
    # type: (CephadmContext, str, str, Union[int, str, None]) -> List[List[str]]
    binds = list()

    if daemon_type == CephIscsi.daemon_type:
        binds.extend(CephIscsi.get_container_binds())
    if daemon_type == CephNvmeof.daemon_type:
        binds.extend(CephNvmeof.get_container_binds())
    elif daemon_type == CustomContainer.daemon_type:
        assert daemon_id
        cc = CustomContainer.init(ctx, fsid, daemon_id)
        data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
        binds.extend(cc.get_container_binds(data_dir))

    return binds


def get_container_mounts(ctx, fsid, daemon_type, daemon_id,
                         no_config=False):
    # type: (CephadmContext, str, str, Union[int, str, None], Optional[bool]) -> Dict[str, str]
    mounts = dict()

    if daemon_type in Ceph.daemons:
        if fsid:
            run_path = os.path.join('/var/run/ceph', fsid)
            if os.path.exists(run_path):
                mounts[run_path] = '/var/run/ceph:z'
            log_dir = get_log_dir(fsid, ctx.log_dir)
            mounts[log_dir] = '/var/log/ceph:z'
            crash_dir = '/var/lib/ceph/%s/crash' % fsid
            if os.path.exists(crash_dir):
                mounts[crash_dir] = '/var/lib/ceph/crash:z'
            if daemon_type != 'crash' and should_log_to_journald(ctx):
                journald_sock_dir = '/run/systemd/journal'
                mounts[journald_sock_dir] = journald_sock_dir

    if daemon_type in Ceph.daemons and daemon_id:
        data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
        if daemon_type == 'rgw':
            cdata_dir = '/var/lib/ceph/radosgw/ceph-rgw.%s' % (daemon_id)
        else:
            cdata_dir = '/var/lib/ceph/%s/ceph-%s' % (daemon_type, daemon_id)
        if daemon_type != 'crash':
            mounts[data_dir] = cdata_dir + ':z'
        if not no_config:
            mounts[data_dir + '/config'] = '/etc/ceph/ceph.conf:z'
        if daemon_type in ['rbd-mirror', 'cephfs-mirror', 'crash', 'ceph-exporter']:
            # these do not search for their keyrings in a data directory
            mounts[data_dir + '/keyring'] = '/etc/ceph/ceph.client.%s.%s.keyring' % (daemon_type, daemon_id)

    if daemon_type in ['mon', 'osd', 'clusterless-ceph-volume']:
        mounts['/dev'] = '/dev'  # FIXME: narrow this down?
        mounts['/run/udev'] = '/run/udev'
    if daemon_type in ['osd', 'clusterless-ceph-volume']:
        mounts['/sys'] = '/sys'  # for numa.cc, pick_address, cgroups, ...
        mounts['/run/lvm'] = '/run/lvm'
        mounts['/run/lock/lvm'] = '/run/lock/lvm'
    if daemon_type == 'osd':
        # selinux-policy in the container may not match the host.
        if HostFacts(ctx).selinux_enabled:
            cluster_dir = f'{ctx.data_dir}/{fsid}'
            selinux_folder = f'{cluster_dir}/selinux'
            if os.path.exists(cluster_dir):
                if not os.path.exists(selinux_folder):
                    os.makedirs(selinux_folder, mode=0o755)
                mounts[selinux_folder] = '/sys/fs/selinux:ro'
            else:
                logger.error(f'Cluster direcotry {cluster_dir} does not exist.')
        mounts['/'] = '/rootfs'

    try:
        if ctx.shared_ceph_folder:  # make easy manager modules/ceph-volume development
            ceph_folder = pathify(ctx.shared_ceph_folder)
            if os.path.exists(ceph_folder):
                mounts[ceph_folder + '/src/ceph-volume/ceph_volume'] = '/usr/lib/python3.6/site-packages/ceph_volume'
                mounts[ceph_folder + '/src/cephadm/cephadm.py'] = '/usr/sbin/cephadm'
                mounts[ceph_folder + '/src/pybind/mgr'] = '/usr/share/ceph/mgr'
                mounts[ceph_folder + '/src/python-common/ceph'] = '/usr/lib/python3.6/site-packages/ceph'
                mounts[ceph_folder + '/monitoring/ceph-mixin/dashboards_out'] = '/etc/grafana/dashboards/ceph-dashboard'
                mounts[ceph_folder + '/monitoring/ceph-mixin/prometheus_alerts.yml'] = '/etc/prometheus/ceph/ceph_default_alerts.yml'
            else:
                logger.error('{}{}{}'.format(termcolor.red,
                                             'Ceph shared source folder does not exist.',
                                             termcolor.end))
    except AttributeError:
        pass

    if daemon_type in Monitoring.components and daemon_id:
        data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
        log_dir = get_log_dir(fsid, ctx.log_dir)
        if daemon_type == 'prometheus':
            mounts[os.path.join(data_dir, 'etc/prometheus')] = '/etc/prometheus:Z'
            mounts[os.path.join(data_dir, 'data')] = '/prometheus:Z'
        elif daemon_type == 'loki':
            mounts[os.path.join(data_dir, 'etc/loki')] = '/etc/loki:Z'
            mounts[os.path.join(data_dir, 'data')] = '/loki:Z'
        elif daemon_type == 'promtail':
            mounts[os.path.join(data_dir, 'etc/promtail')] = '/etc/promtail:Z'
            mounts[log_dir] = '/var/log/ceph:z'
            mounts[os.path.join(data_dir, 'data')] = '/promtail:Z'
        elif daemon_type == 'node-exporter':
            mounts[os.path.join(data_dir, 'etc/node-exporter')] = '/etc/node-exporter:Z'
            mounts['/proc'] = '/host/proc:ro'
            mounts['/sys'] = '/host/sys:ro'
            mounts['/'] = '/rootfs:ro'
        elif daemon_type == 'grafana':
            mounts[os.path.join(data_dir, 'etc/grafana/grafana.ini')] = '/etc/grafana/grafana.ini:Z'
            mounts[os.path.join(data_dir, 'etc/grafana/provisioning/datasources')] = '/etc/grafana/provisioning/datasources:Z'
            mounts[os.path.join(data_dir, 'etc/grafana/certs')] = '/etc/grafana/certs:Z'
            mounts[os.path.join(data_dir, 'data/grafana.db')] = '/var/lib/grafana/grafana.db:Z'
        elif daemon_type == 'alertmanager':
            mounts[os.path.join(data_dir, 'etc/alertmanager')] = '/etc/alertmanager:Z'

    if daemon_type == NFSGanesha.daemon_type:
        assert daemon_id
        data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
        nfs_ganesha = NFSGanesha.init(ctx, fsid, daemon_id)
        mounts.update(nfs_ganesha.get_container_mounts(data_dir))

    if daemon_type == HAproxy.daemon_type:
        assert daemon_id
        data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
        mounts.update(HAproxy.get_container_mounts(data_dir))

    if daemon_type == CephNvmeof.daemon_type:
        assert daemon_id
        data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
        mounts.update(CephNvmeof.get_container_mounts(data_dir))

    if daemon_type == CephIscsi.daemon_type:
        assert daemon_id
        data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
        # Removes ending ".tcmu" from data_dir a tcmu-runner uses the same data_dir
        # as rbd-runner-api
        if data_dir.endswith('.tcmu'):
            data_dir = re.sub(r'\.tcmu$', '', data_dir)
        log_dir = get_log_dir(fsid, ctx.log_dir)
        mounts.update(CephIscsi.get_container_mounts(data_dir, log_dir))

    if daemon_type == Keepalived.daemon_type:
        assert daemon_id
        data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
        mounts.update(Keepalived.get_container_mounts(data_dir))

    if daemon_type == CustomContainer.daemon_type:
        assert daemon_id
        cc = CustomContainer.init(ctx, fsid, daemon_id)
        data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
        mounts.update(cc.get_container_mounts(data_dir))

    # Modifications podman makes to /etc/hosts causes issues with
    # certain daemons (specifically referencing "host.containers.internal" entry
    # being added to /etc/hosts in this case). To avoid that, but still
    # allow users to use /etc/hosts for hostname resolution, we can
    # mount the host's /etc/hosts file.
    # https://tracker.ceph.com/issues/58532
    # https://tracker.ceph.com/issues/57018
    if isinstance(ctx.container_engine, Podman):
        if os.path.exists('/etc/hosts'):
            if '/etc/hosts' not in mounts:
                mounts['/etc/hosts'] = '/etc/hosts:ro'

    return mounts


def get_ceph_volume_container(ctx: CephadmContext,
                              privileged: bool = True,
                              cname: str = '',
                              volume_mounts: Dict[str, str] = {},
                              bind_mounts: Optional[List[List[str]]] = None,
                              args: List[str] = [],
                              envs: Optional[List[str]] = None) -> 'CephContainer':
    if envs is None:
        envs = []
    envs.append('CEPH_VOLUME_SKIP_RESTORECON=yes')
    envs.append('CEPH_VOLUME_DEBUG=1')

    return CephContainer(
        ctx,
        image=ctx.image,
        entrypoint='/usr/sbin/ceph-volume',
        args=args,
        volume_mounts=volume_mounts,
        bind_mounts=bind_mounts,
        envs=envs,
        privileged=privileged,
        cname=cname,
        memory_request=ctx.memory_request,
        memory_limit=ctx.memory_limit,
    )


def set_pids_limit_unlimited(ctx: CephadmContext, container_args: List[str]) -> None:
    # set container's pids-limit to unlimited rather than default (Docker 4096 / Podman 2048)
    # Useful for daemons like iscsi where the default pids-limit limits the number of luns
    # per iscsi target or rgw where increasing the rgw_thread_pool_size to a value near
    # the default pids-limit may cause the container to crash.
    if (
        isinstance(ctx.container_engine, Podman)
        and ctx.container_engine.version >= PIDS_LIMIT_UNLIMITED_PODMAN_VERSION
    ):
        container_args.append('--pids-limit=-1')
    else:
        container_args.append('--pids-limit=0')


def get_container(ctx: CephadmContext,
                  fsid: str, daemon_type: str, daemon_id: Union[int, str],
                  privileged: bool = False,
                  ptrace: bool = False,
                  container_args: Optional[List[str]] = None) -> 'CephContainer':
    entrypoint: str = ''
    name: str = ''
    ceph_args: List[str] = []
    envs: List[str] = []
    host_network: bool = True

    if daemon_type in Ceph.daemons:
        envs.append('TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES=134217728')
    if container_args is None:
        container_args = []
    if daemon_type in Ceph.daemons or daemon_type in Ceph.gateways:
        set_pids_limit_unlimited(ctx, container_args)
    if daemon_type in ['mon', 'osd']:
        # mon and osd need privileged in order for libudev to query devices
        privileged = True
    if daemon_type == 'rgw':
        entrypoint = '/usr/bin/radosgw'
        name = 'client.rgw.%s' % daemon_id
    elif daemon_type == 'rbd-mirror':
        entrypoint = '/usr/bin/rbd-mirror'
        name = 'client.rbd-mirror.%s' % daemon_id
    elif daemon_type == 'cephfs-mirror':
        entrypoint = '/usr/bin/cephfs-mirror'
        name = 'client.cephfs-mirror.%s' % daemon_id
    elif daemon_type == 'crash':
        entrypoint = '/usr/bin/ceph-crash'
        name = 'client.crash.%s' % daemon_id
    elif daemon_type in ['mon', 'mgr', 'mds', 'osd']:
        entrypoint = '/usr/bin/ceph-' + daemon_type
        name = '%s.%s' % (daemon_type, daemon_id)
    elif daemon_type in Monitoring.components:
        entrypoint = ''
    elif daemon_type in Tracing.components:
        entrypoint = ''
        name = '%s.%s' % (daemon_type, daemon_id)
        config = fetch_configs(ctx)
        Tracing.set_configuration(config, daemon_type)
        envs.extend(Tracing.components[daemon_type].get('envs', []))
    elif daemon_type == NFSGanesha.daemon_type:
        entrypoint = NFSGanesha.entrypoint
        name = '%s.%s' % (daemon_type, daemon_id)
        envs.extend(NFSGanesha.get_container_envs())
    elif daemon_type == CephExporter.daemon_type:
        entrypoint = CephExporter.entrypoint
        name = 'client.ceph-exporter.%s' % daemon_id
    elif daemon_type == HAproxy.daemon_type:
        name = '%s.%s' % (daemon_type, daemon_id)
        container_args.extend(['--user=root'])  # haproxy 2.4 defaults to a different user
    elif daemon_type == Keepalived.daemon_type:
        name = '%s.%s' % (daemon_type, daemon_id)
        envs.extend(Keepalived.get_container_envs())
        container_args.extend(['--cap-add=NET_ADMIN', '--cap-add=NET_RAW'])
    elif daemon_type == CephNvmeof.daemon_type:
        name = '%s.%s' % (daemon_type, daemon_id)
        container_args.extend(['--ulimit', 'memlock=-1:-1'])
        container_args.extend(['--ulimit', 'nofile=10240'])
        container_args.extend(['--cap-add=SYS_ADMIN', '--cap-add=CAP_SYS_NICE'])
    elif daemon_type == CephIscsi.daemon_type:
        entrypoint = CephIscsi.entrypoint
        name = '%s.%s' % (daemon_type, daemon_id)
        # So the container can modprobe iscsi_target_mod and have write perms
        # to configfs we need to make this a privileged container.
        privileged = True
    elif daemon_type == CustomContainer.daemon_type:
        cc = CustomContainer.init(ctx, fsid, daemon_id)
        entrypoint = cc.entrypoint
        host_network = False
        envs.extend(cc.get_container_envs())
        container_args.extend(cc.get_container_args())

    if daemon_type in Monitoring.components:
        uid, gid = extract_uid_gid_monitoring(ctx, daemon_type)
        monitoring_args = [
            '--user',
            str(uid),
            # FIXME: disable cpu/memory limits for the time being (not supported
            # by ubuntu 18.04 kernel!)
        ]
        container_args.extend(monitoring_args)
        if daemon_type == 'node-exporter':
            # in order to support setting '--path.procfs=/host/proc','--path.sysfs=/host/sys',
            # '--path.rootfs=/rootfs' for node-exporter we need to disable selinux separation
            # between the node-exporter container and the host to avoid selinux denials
            container_args.extend(['--security-opt', 'label=disable'])
    elif daemon_type == 'crash':
        ceph_args = ['-n', name]
    elif daemon_type in Ceph.daemons:
        ceph_args = ['-n', name, '-f']
    elif daemon_type == SNMPGateway.daemon_type:
        sg = SNMPGateway.init(ctx, fsid, daemon_id)
        container_args.append(
            f'--env-file={sg.conf_file_path}'
        )

    # if using podman, set -d, --conmon-pidfile & --cidfile flags
    # so service can have Type=Forking
    if isinstance(ctx.container_engine, Podman):
        runtime_dir = '/run'
        container_args.extend([
            '-d', '--log-driver', 'journald',
            '--conmon-pidfile',
            runtime_dir + '/ceph-%s@%s.%s.service-pid' % (fsid, daemon_type, daemon_id),
            '--cidfile',
            runtime_dir + '/ceph-%s@%s.%s.service-cid' % (fsid, daemon_type, daemon_id),
        ])
        if ctx.container_engine.version >= CGROUPS_SPLIT_PODMAN_VERSION and not ctx.no_cgroups_split:
            container_args.append('--cgroups=split')
        # if /etc/hosts doesn't exist, we can be confident
        # users aren't using it for host name resolution
        # and adding --no-hosts avoids bugs created in certain daemons
        # by modifications podman makes to /etc/hosts
        # https://tracker.ceph.com/issues/58532
        # https://tracker.ceph.com/issues/57018
        if not os.path.exists('/etc/hosts'):
            container_args.extend(['--no-hosts'])

    return CephContainer.for_daemon(
        ctx,
        fsid=fsid,
        daemon_type=daemon_type,
        daemon_id=str(daemon_id),
        entrypoint=entrypoint,
        args=ceph_args + get_daemon_args(ctx, fsid, daemon_type, daemon_id),
        container_args=container_args,
        volume_mounts=get_container_mounts(ctx, fsid, daemon_type, daemon_id),
        bind_mounts=get_container_binds(ctx, fsid, daemon_type, daemon_id),
        envs=envs,
        privileged=privileged,
        ptrace=ptrace,
        host_network=host_network,
    )


def extract_uid_gid(ctx, img='', file_path='/var/lib/ceph'):
    # type: (CephadmContext, str, Union[str, List[str]]) -> Tuple[int, int]

    if not img:
        img = ctx.image

    if isinstance(file_path, str):
        paths = [file_path]
    else:
        paths = file_path

    ex: Optional[Tuple[str, RuntimeError]] = None

    for fp in paths:
        try:
            out = CephContainer(
                ctx,
                image=img,
                entrypoint='stat',
                args=['-c', '%u %g', fp]
            ).run(verbosity=CallVerbosity.QUIET_UNLESS_ERROR)
            uid, gid = out.split(' ')
            return int(uid), int(gid)
        except RuntimeError as e:
            ex = (fp, e)
    if ex:
        raise Error(f'Failed to extract uid/gid for path {ex[0]}: {ex[1]}')

    raise RuntimeError('uid/gid not found')


def deploy_daemon(ctx: CephadmContext, fsid: str, daemon_type: str,
                  daemon_id: Union[int, str], c: Optional['CephContainer'],
                  uid: int, gid: int, config: Optional[str] = None,
                  keyring: Optional[str] = None, osd_fsid: Optional[str] = None,
                  deployment_type: DeploymentType = DeploymentType.DEFAULT,
                  endpoints: Optional[List[EndPoint]] = None) -> None:

    endpoints = endpoints or []
    # only check port in use if fresh deployment since service
    # we are redeploying/reconfiguring will already be using the port
    if deployment_type == DeploymentType.DEFAULT:
        if any([port_in_use(ctx, e) for e in endpoints]):
            if daemon_type == 'mgr':
                # non-fatal for mgr when we are in mgr_standby_modules=false, but we can't
                # tell whether that is the case here.
                logger.warning(
                    f"ceph-mgr TCP port(s) {','.join(map(str, endpoints))} already in use"
                )
            else:
                raise Error("TCP Port(s) '{}' required for {} already in use".format(','.join(map(str, endpoints)), daemon_type))

    data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
    if deployment_type == DeploymentType.RECONFIG and not os.path.exists(data_dir):
        raise Error('cannot reconfig, data path %s does not exist' % data_dir)
    if daemon_type == 'mon' and not os.path.exists(data_dir):
        assert config
        assert keyring
        # tmp keyring file
        tmp_keyring = write_tmp(keyring, uid, gid)

        # tmp config file
        tmp_config = write_tmp(config, uid, gid)

        # --mkfs
        create_daemon_dirs(ctx, fsid, daemon_type, daemon_id, uid, gid)
        mon_dir = get_data_dir(fsid, ctx.data_dir, 'mon', daemon_id)
        log_dir = get_log_dir(fsid, ctx.log_dir)
        CephContainer(
            ctx,
            image=ctx.image,
            entrypoint='/usr/bin/ceph-mon',
            args=[
                '--mkfs',
                '-i', str(daemon_id),
                '--fsid', fsid,
                '-c', '/tmp/config',
                '--keyring', '/tmp/keyring',
            ] + get_daemon_args(ctx, fsid, 'mon', daemon_id),
            volume_mounts={
                log_dir: '/var/log/ceph:z',
                mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % (daemon_id),
                tmp_keyring.name: '/tmp/keyring:z',
                tmp_config.name: '/tmp/config:z',
            },
        ).run()

        # write conf
        with write_new(mon_dir + '/config', owner=(uid, gid)) as f:
            f.write(config)
    else:
        # dirs, conf, keyring
        create_daemon_dirs(
            ctx,
            fsid, daemon_type, daemon_id,
            uid, gid,
            config, keyring)

    # only write out unit files and start daemon
    # with systemd if this is not a reconfig
    if deployment_type != DeploymentType.RECONFIG:
        if daemon_type == CephadmAgent.daemon_type:
            config_js = fetch_configs(ctx)
            assert isinstance(config_js, dict)

            cephadm_agent = CephadmAgent(ctx, fsid, daemon_id)
            cephadm_agent.deploy_daemon_unit(config_js)
        else:
            if c:
                deploy_daemon_units(ctx, fsid, uid, gid, daemon_type, daemon_id,
                                    c, osd_fsid=osd_fsid, endpoints=endpoints)
            else:
                raise RuntimeError('attempting to deploy a daemon without a container image')

    if not os.path.exists(data_dir + '/unit.created'):
        with write_new(data_dir + '/unit.created', owner=(uid, gid)) as f:
            f.write('mtime is time the daemon deployment was created\n')

    with write_new(data_dir + '/unit.configured', owner=(uid, gid)) as f:
        f.write('mtime is time we were last configured\n')

    update_firewalld(ctx, daemon_type)

    # Open ports explicitly required for the daemon
    if endpoints:
        fw = Firewalld(ctx)
        fw.open_ports([e.port for e in endpoints] + fw.external_ports.get(daemon_type, []))
        fw.apply_rules()

    # If this was a reconfig and the daemon is not a Ceph daemon, restart it
    # so it can pick up potential changes to its configuration files
    if deployment_type == DeploymentType.RECONFIG and daemon_type not in Ceph.daemons:
        # ceph daemons do not need a restart; others (presumably) do to pick
        # up the new config
        call_throws(ctx, ['systemctl', 'reset-failed',
                          get_unit_name(fsid, daemon_type, daemon_id)])
        call_throws(ctx, ['systemctl', 'restart',
                          get_unit_name(fsid, daemon_type, daemon_id)])


def _write_container_cmd_to_bash(ctx, file_obj, container, comment=None, background=False):
    # type: (CephadmContext, IO[str], CephContainer, Optional[str], Optional[bool]) -> None
    if comment:
        # Sometimes adding a comment, especially if there are multiple containers in one
        # unit file, makes it easier to read and grok.
        file_obj.write('# ' + comment + '\n')
    # Sometimes, adding `--rm` to a run_cmd doesn't work. Let's remove the container manually
    file_obj.write('! ' + ' '.join(container.rm_cmd(old_cname=True)) + ' 2> /dev/null\n')
    file_obj.write('! ' + ' '.join(container.rm_cmd()) + ' 2> /dev/null\n')
    # Sometimes, `podman rm` doesn't find the container. Then you'll have to add `--storage`
    if isinstance(ctx.container_engine, Podman):
        file_obj.write(
            '! '
            + ' '.join([shlex.quote(a) for a in container.rm_cmd(storage=True)])
            + ' 2> /dev/null\n')
        file_obj.write(
            '! '
            + ' '.join([shlex.quote(a) for a in container.rm_cmd(old_cname=True, storage=True)])
            + ' 2> /dev/null\n')

    # container run command
    file_obj.write(
        ' '.join([shlex.quote(a) for a in container.run_cmd()])
        + (' &' if background else '') + '\n')


def clean_cgroup(ctx: CephadmContext, fsid: str, unit_name: str) -> None:
    # systemd may fail to cleanup cgroups from previous stopped unit, which will cause next "systemctl start" to fail.
    # see https://tracker.ceph.com/issues/50998

    CGROUPV2_PATH = Path('/sys/fs/cgroup')
    if not (CGROUPV2_PATH / 'system.slice').exists():
        # Only unified cgroup is affected, skip if not the case
        return

    slice_name = 'system-ceph\\x2d{}.slice'.format(fsid.replace('-', '\\x2d'))
    cg_path = CGROUPV2_PATH / 'system.slice' / slice_name / f'{unit_name}.service'
    if not cg_path.exists():
        return

    def cg_trim(path: Path) -> None:
        for p in path.iterdir():
            if p.is_dir():
                cg_trim(p)
        path.rmdir()
    try:
        cg_trim(cg_path)
    except OSError:
        logger.warning(f'Failed to trim old cgroups {cg_path}')


def deploy_daemon_units(
    ctx: CephadmContext,
    fsid: str,
    uid: int,
    gid: int,
    daemon_type: str,
    daemon_id: Union[int, str],
    c: 'CephContainer',
    enable: bool = True,
    start: bool = True,
    osd_fsid: Optional[str] = None,
    endpoints: Optional[List[EndPoint]] = None,
) -> None:
    # cmd

    def add_stop_actions(f: TextIO, timeout: Optional[int]) -> None:
        # following generated script basically checks if the container exists
        # before stopping it. Exit code will be success either if it doesn't
        # exist or if it exists and is stopped successfully.
        container_exists = f'{ctx.container_engine.path} inspect %s &>/dev/null'
        f.write(f'! {container_exists % c.old_cname} || {" ".join(c.stop_cmd(old_cname=True, timeout=timeout))} \n')
        f.write(f'! {container_exists % c.cname} || {" ".join(c.stop_cmd(timeout=timeout))} \n')

    data_dir = get_data_dir(fsid, ctx.data_dir, daemon_type, daemon_id)
    run_file_path = data_dir + '/unit.run'
    meta_file_path = data_dir + '/unit.meta'
    with write_new(run_file_path) as f, write_new(meta_file_path) as metaf:

        f.write('set -e\n')

        if daemon_type in Ceph.daemons:
            install_path = find_program('install')
            f.write('{install_path} -d -m0770 -o {uid} -g {gid} /var/run/ceph/{fsid}\n'.format(install_path=install_path, fsid=fsid, uid=uid, gid=gid))

        # pre-start cmd(s)
        if daemon_type == 'osd':
            # osds have a pre-start step
            assert osd_fsid
            simple_fn = os.path.join('/etc/ceph/osd',
                                     '%s-%s.json.adopted-by-cephadm' % (daemon_id, osd_fsid))
            if os.path.exists(simple_fn):
                f.write('# Simple OSDs need chown on startup:\n')
                for n in ['block', 'block.db', 'block.wal']:
                    p = os.path.join(data_dir, n)
                    f.write('[ ! -L {p} ] || chown {uid}:{gid} {p}\n'.format(p=p, uid=uid, gid=gid))
            else:
                # if ceph-volume does not support 'ceph-volume activate', we must
                # do 'ceph-volume lvm activate'.
                test_cv = get_ceph_volume_container(
                    ctx,
                    args=['activate', '--bad-option'],
                    volume_mounts=get_container_mounts(ctx, fsid, daemon_type, daemon_id),
                    bind_mounts=get_container_binds(ctx, fsid, daemon_type, daemon_id),
                    cname='ceph-%s-%s.%s-activate-test' % (fsid, daemon_type, daemon_id),
                )
                out, err, ret = call(ctx, test_cv.run_cmd(), verbosity=CallVerbosity.SILENT)
                #  bad: ceph-volume: error: unrecognized arguments: activate --bad-option
                # good: ceph-volume: error: unrecognized arguments: --bad-option
                if 'unrecognized arguments: activate' in err:
                    # older ceph-volume without top-level activate or --no-tmpfs
                    cmd = [
                        'lvm', 'activate',
                        str(daemon_id), osd_fsid,
                        '--no-systemd',
                    ]
                else:
                    cmd = [
                        'activate',
                        '--osd-id', str(daemon_id),
                        '--osd-uuid', osd_fsid,
                        '--no-systemd',
                        '--no-tmpfs',
                    ]

                prestart = get_ceph_volume_container(
                    ctx,
                    args=cmd,
                    volume_mounts=get_container_mounts(ctx, fsid, daemon_type, daemon_id),
                    bind_mounts=get_container_binds(ctx, fsid, daemon_type, daemon_id),
                    cname='ceph-%s-%s.%s-activate' % (fsid, daemon_type, daemon_id),
                )
                _write_container_cmd_to_bash(ctx, f, prestart, 'LVM OSDs use ceph-volume lvm activate')
        elif daemon_type == CephIscsi.daemon_type:
            f.write(' '.join(CephIscsi.configfs_mount_umount(data_dir, mount=True)) + '\n')
            ceph_iscsi = CephIscsi.init(ctx, fsid, daemon_id)
            tcmu_container = ceph_iscsi.get_tcmu_runner_container()
            _write_container_cmd_to_bash(ctx, f, tcmu_container, 'iscsi tcmu-runner container', background=True)

        _write_container_cmd_to_bash(ctx, f, c, '%s.%s' % (daemon_type, str(daemon_id)))

        # some metadata about the deploy
        meta: Dict[str, Any] = fetch_meta(ctx)
        meta.update({
            'memory_request': int(ctx.memory_request) if ctx.memory_request else None,
            'memory_limit': int(ctx.memory_limit) if ctx.memory_limit else None,
        })
        if not meta.get('ports'):
            if endpoints:
                meta['ports'] = [e.port for e in endpoints]
            else:
                meta['ports'] = []
        metaf.write(json.dumps(meta, indent=4) + '\n')

    timeout = 30 if daemon_type == 'osd' else None
    # post-stop command(s)
    with write_new(data_dir + '/unit.poststop') as f:
        # this is a fallback to eventually stop any underlying container that was not stopped properly by unit.stop,
        # this could happen in very slow setups as described in the issue https://tracker.ceph.com/issues/58242.
        add_stop_actions(cast(TextIO, f), timeout)
        if daemon_type == 'osd':
            assert osd_fsid
            poststop = get_ceph_volume_container(
                ctx,
                args=[
                    'lvm', 'deactivate',
                    str(daemon_id), osd_fsid,
                ],
                volume_mounts=get_container_mounts(ctx, fsid, daemon_type, daemon_id),
                bind_mounts=get_container_binds(ctx, fsid, daemon_type, daemon_id),
                cname='ceph-%s-%s.%s-deactivate' % (fsid, daemon_type,
                                                    daemon_id),
            )
            _write_container_cmd_to_bash(ctx, f, poststop, 'deactivate osd')
        elif daemon_type == CephIscsi.daemon_type:
            # make sure we also stop the tcmu container
            runtime_dir = '/run'
            ceph_iscsi = CephIscsi.init(ctx, fsid, daemon_id)
            tcmu_container = ceph_iscsi.get_tcmu_runner_container()
            f.write('! ' + ' '.join(tcmu_container.stop_cmd()) + '\n')
            f.write('! ' + 'rm ' + runtime_dir + '/ceph-%s@%s.%s.service-pid' % (fsid, daemon_type, str(daemon_id) + '.tcmu') + '\n')
            f.write('! ' + 'rm ' + runtime_dir + '/ceph-%s@%s.%s.service-cid' % (fsid, daemon_type, str(daemon_id) + '.tcmu') + '\n')
            f.write(' '.join(CephIscsi.configfs_mount_umount(data_dir, mount=False)) + '\n')

    # post-stop command(s)
    with write_new(data_dir + '/unit.stop') as f:
        add_stop_actions(cast(TextIO, f), timeout)

    if c:
        with write_new(data_dir + '/unit.image') as f:
            f.write(c.image + '\n')

    # sysctl
    install_sysctl(ctx, fsid, daemon_type)

    # systemd
    install_base_units(ctx, fsid)
    unit = get_unit_file(ctx, fsid)
    unit_file = 'ceph-%s@.service' % (fsid)
    with write_new(ctx.unit_dir + '/' + unit_file, perms=None) as f:
        f.write(unit)
    call_throws(ctx, ['systemctl', 'daemon-reload'])

    unit_name = get_unit_name(fsid, daemon_type, daemon_id)
    call(ctx, ['systemctl', 'stop', unit_name],
         verbosity=CallVerbosity.DEBUG)
    call(ctx, ['systemctl', 'reset-failed', unit_name],
         verbosity=CallVerbosity.DEBUG)
    if enable:
        call_throws(ctx, ['systemctl', 'enable', unit_name])
    if start:
        clean_cgroup(ctx, fsid, unit_name)
        call_throws(ctx, ['systemctl', 'start', unit_name])


class Firewalld(object):

    # for specifying ports we should always open when opening
    # ports for a daemon of that type. Main use case is for ports
    # that we should open when deploying the daemon type but that
    # the daemon itself may not necessarily need to bind to the port.
    # This needs to be handed differently as we don't want to fail
    # deployment if the port cannot be bound to but we still want to
    # open the port in the firewall.
    external_ports: Dict[str, List[int]] = {
        'iscsi': [3260]  # 3260 is the well known iSCSI port
    }

    def __init__(self, ctx):
        # type: (CephadmContext) -> None
        self.ctx = ctx
        self.available = self.check()

    def check(self):
        # type: () -> bool
        self.cmd = find_executable('firewall-cmd')
        if not self.cmd:
            logger.debug('firewalld does not appear to be present')
            return False
        (enabled, state, _) = check_unit(self.ctx, 'firewalld.service')
        if not enabled:
            logger.debug('firewalld.service is not enabled')
            return False
        if state != 'running':
            logger.debug('firewalld.service is not running')
            return False

        logger.info('firewalld ready')
        return True

    def enable_service_for(self, daemon_type):
        # type: (str) -> None
        if not self.available:
            logger.debug('Not possible to enable service <%s>. firewalld.service is not available' % daemon_type)
            return

        if daemon_type == 'mon':
            svc = 'ceph-mon'
        elif daemon_type in ['mgr', 'mds', 'osd']:
            svc = 'ceph'
        elif daemon_type == NFSGanesha.daemon_type:
            svc = 'nfs'
        else:
            return

        if not self.cmd:
            raise RuntimeError('command not defined')

        out, err, ret = call(self.ctx, [self.cmd, '--permanent', '--query-service', svc], verbosity=CallVerbosity.DEBUG)
        if ret:
            logger.info('Enabling firewalld service %s in current zone...' % svc)
            out, err, ret = call(self.ctx, [self.cmd, '--permanent', '--add-service', svc])
            if ret:
                raise RuntimeError(
                    'unable to add service %s to current zone: %s' % (svc, err))
        else:
            logger.debug('firewalld service %s is enabled in current zone' % svc)

    def open_ports(self, fw_ports):
        # type: (List[int]) -> None
        if not self.available:
            logger.debug('Not possible to open ports <%s>. firewalld.service is not available' % fw_ports)
            return

        if not self.cmd:
            raise RuntimeError('command not defined')

        for port in fw_ports:
            tcp_port = str(port) + '/tcp'
            out, err, ret = call(self.ctx, [self.cmd, '--permanent', '--query-port', tcp_port], verbosity=CallVerbosity.DEBUG)
            if ret:
                logger.info('Enabling firewalld port %s in current zone...' % tcp_port)
                out, err, ret = call(self.ctx, [self.cmd, '--permanent', '--add-port', tcp_port])
                if ret:
                    raise RuntimeError('unable to add port %s to current zone: %s' %
                                       (tcp_port, err))
            else:
                logger.debug('firewalld port %s is enabled in current zone' % tcp_port)

    def close_ports(self, fw_ports):
        # type: (List[int]) -> None
        if not self.available:
            logger.debug('Not possible to close ports <%s>. firewalld.service is not available' % fw_ports)
            return

        if not self.cmd:
            raise RuntimeError('command not defined')

        for port in fw_ports:
            tcp_port = str(port) + '/tcp'
            out, err, ret = call(self.ctx, [self.cmd, '--permanent', '--query-port', tcp_port], verbosity=CallVerbosity.DEBUG)
            if not ret:
                logger.info('Disabling port %s in current zone...' % tcp_port)
                out, err, ret = call(self.ctx, [self.cmd, '--permanent', '--remove-port', tcp_port])
                if ret:
                    raise RuntimeError('unable to remove port %s from current zone: %s' %
                                       (tcp_port, err))
                else:
                    logger.info(f'Port {tcp_port} disabled')
            else:
                logger.info(f'firewalld port {tcp_port} already closed')

    def apply_rules(self):
        # type: () -> None
        if not self.available:
            return

        if not self.cmd:
            raise RuntimeError('command not defined')

        call_throws(self.ctx, [self.cmd, '--reload'])


def update_firewalld(ctx, daemon_type):
    # type: (CephadmContext, str) -> None
    if not ('skip_firewalld' in ctx and ctx.skip_firewalld):
        firewall = Firewalld(ctx)
        firewall.enable_service_for(daemon_type)
        firewall.apply_rules()


def install_sysctl(ctx: CephadmContext, fsid: str, daemon_type: str) -> None:
    """
    Set up sysctl settings
    """
    def _write(conf: Path, lines: List[str]) -> None:
        lines = [
            '# created by cephadm',
            '',
            *lines,
            '',
        ]
        with write_new(conf, owner=None, perms=None) as f:
            f.write('\n'.join(lines))

    conf = Path(ctx.sysctl_dir).joinpath(f'90-ceph-{fsid}-{daemon_type}.conf')
    lines: List = []

    if daemon_type == 'osd':
        lines = OSD.get_sysctl_settings()
    elif daemon_type == 'haproxy':
        lines = HAproxy.get_sysctl_settings()
    elif daemon_type == 'keepalived':
        lines = Keepalived.get_sysctl_settings()
    elif daemon_type == CephNvmeof.daemon_type:
        lines = CephNvmeof.get_sysctl_settings()
    lines = filter_sysctl_settings(ctx, lines)

    # apply the sysctl settings
    if lines:
        Path(ctx.sysctl_dir).mkdir(mode=0o755, exist_ok=True)
        _write(conf, lines)
        call_throws(ctx, ['sysctl', '--system'])


def sysctl_get(ctx: CephadmContext, variable: str) -> Union[str, None]:
    """
    Read a sysctl setting by executing 'sysctl -b {variable}'
    """
    out, err, code = call(ctx, ['sysctl', '-b', variable])
    return out or None


def filter_sysctl_settings(ctx: CephadmContext, lines: List[str]) -> List[str]:
    """
    Given a list of sysctl settings, examine the system's current configuration
    and return those which are not currently set as described.
    """
    def test_setting(desired_line: str) -> bool:
        # Remove any comments
        comment_start = desired_line.find('#')
        if comment_start != -1:
            desired_line = desired_line[:comment_start]
        desired_line = desired_line.strip()
        if not desired_line or desired_line.isspace():
            return False
        setting, desired_value = map(lambda s: s.strip(), desired_line.split('='))
        if not setting or not desired_value:
            return False
        actual_value = sysctl_get(ctx, setting)
        return desired_value != actual_value
    return list(filter(test_setting, lines))


def migrate_sysctl_dir(ctx: CephadmContext, fsid: str) -> None:
    """
    Cephadm once used '/usr/lib/sysctl.d' for storing sysctl configuration.
    This moves it to '/etc/sysctl.d'.
    """
    deprecated_location: str = '/usr/lib/sysctl.d'
    deprecated_confs: List[str] = glob(f'{deprecated_location}/90-ceph-{fsid}-*.conf')
    if not deprecated_confs:
        return

    file_count: int = len(deprecated_confs)
    logger.info(f'Found sysctl {file_count} files in deprecated location {deprecated_location}. Starting Migration.')
    for conf in deprecated_confs:
        try:
            shutil.move(conf, ctx.sysctl_dir)
            file_count -= 1
        except shutil.Error as err:
            if str(err).endswith('already exists'):
                logger.warning(f'Destination file already exists. Deleting {conf}.')
                try:
                    os.unlink(conf)
                    file_count -= 1
                except OSError as del_err:
                    logger.warning(f'Could not remove {conf}: {del_err}.')
            else:
                logger.warning(f'Could not move {conf} from {deprecated_location} to {ctx.sysctl_dir}: {err}')

    # Log successful migration
    if file_count == 0:
        logger.info(f'Successfully migrated sysctl config to {ctx.sysctl_dir}.')
        return

    # Log partially successful / unsuccessful migration
    files_processed: int = len(deprecated_confs)
    if file_count < files_processed:
        status: str = f'partially successful (failed {file_count}/{files_processed})'
    elif file_count == files_processed:
        status = 'unsuccessful'
    logger.warning(f'Migration of sysctl configuration {status}. You may want to perform a migration manually.')


def install_base_units(ctx, fsid):
    # type: (CephadmContext, str) -> None
    """
    Set up ceph.target and ceph-$fsid.target units.
    """
    # global unit
    existed = os.path.exists(ctx.unit_dir + '/ceph.target')
    with write_new(ctx.unit_dir + '/ceph.target', perms=None) as f:
        f.write('[Unit]\n'
                'Description=All Ceph clusters and services\n'
                '\n'
                '[Install]\n'
                'WantedBy=multi-user.target\n')
    if not existed:
        # we disable before enable in case a different ceph.target
        # (from the traditional package) is present; while newer
        # systemd is smart enough to disable the old
        # (/lib/systemd/...) and enable the new (/etc/systemd/...),
        # some older versions of systemd error out with EEXIST.
        call_throws(ctx, ['systemctl', 'disable', 'ceph.target'])
        call_throws(ctx, ['systemctl', 'enable', 'ceph.target'])
        call_throws(ctx, ['systemctl', 'start', 'ceph.target'])

    # cluster unit
    existed = os.path.exists(ctx.unit_dir + '/ceph-%s.target' % fsid)
    with write_new(ctx.unit_dir + f'/ceph-{fsid}.target', perms=None) as f:
        f.write(
            '[Unit]\n'
            'Description=Ceph cluster {fsid}\n'
            'PartOf=ceph.target\n'
            'Before=ceph.target\n'
            '\n'
            '[Install]\n'
            'WantedBy=multi-user.target ceph.target\n'.format(
                fsid=fsid)
        )
    if not existed:
        call_throws(ctx, ['systemctl', 'enable', 'ceph-%s.target' % fsid])
        call_throws(ctx, ['systemctl', 'start', 'ceph-%s.target' % fsid])

    # don't overwrite file in order to allow users to manipulate it
    if os.path.exists(ctx.logrotate_dir + f'/ceph-{fsid}'):
        return

    # logrotate for the cluster
    with write_new(ctx.logrotate_dir + f'/ceph-{fsid}', perms=None) as f:
        """
        This is a bit sloppy in that the killall/pkill will touch all ceph daemons
        in all containers, but I don't see an elegant way to send SIGHUP *just* to
        the daemons for this cluster.  (1) systemd kill -s will get the signal to
        podman, but podman will exit.  (2) podman kill will get the signal to the
        first child (bash), but that isn't the ceph daemon.  This is simpler and
        should be harmless.
        """
        targets: List[str] = [
            'ceph-mon',
            'ceph-mgr',
            'ceph-mds',
            'ceph-osd',
            'ceph-fuse',
            'radosgw',
            'rbd-mirror',
            'cephfs-mirror',
            'tcmu-runner'
        ]

        f.write("""# created by cephadm
/var/log/ceph/%s/*.log {
    rotate 7
    daily
    compress
    sharedscripts
    postrotate
        killall -q -1 %s || pkill -1 -x '%s' || true
    endscript
    missingok
    notifempty
    su root root
}
""" % (fsid, ' '.join(targets), '|'.join(targets)))


def get_unit_file(ctx, fsid):
    # type: (CephadmContext, str) -> str
    extra_args = ''
    if isinstance(ctx.container_engine, Podman):
        extra_args = ('ExecStartPre=-/bin/rm -f %t/%n-pid %t/%n-cid\n'
                      'ExecStopPost=-/bin/rm -f %t/%n-pid %t/%n-cid\n'
                      'Type=forking\n'
                      'PIDFile=%t/%n-pid\n')
        if ctx.container_engine.version >= CGROUPS_SPLIT_PODMAN_VERSION:
            extra_args += 'Delegate=yes\n'

    docker = isinstance(ctx.container_engine, Docker)
    u = """# generated by cephadm
[Unit]
Description=Ceph %i for {fsid}

# According to:
#   http://www.freedesktop.org/wiki/Software/systemd/NetworkTarget
# these can be removed once ceph-mon will dynamically change network
# configuration.
After=network-online.target local-fs.target time-sync.target{docker_after}
Wants=network-online.target local-fs.target time-sync.target
{docker_requires}

PartOf=ceph-{fsid}.target
Before=ceph-{fsid}.target

[Service]
LimitNOFILE=1048576
LimitNPROC=1048576
EnvironmentFile=-/etc/environment
ExecStart=/bin/bash {data_dir}/{fsid}/%i/unit.run
ExecStop=-/bin/bash -c 'bash {data_dir}/{fsid}/%i/unit.stop'
ExecStopPost=-/bin/bash {data_dir}/{fsid}/%i/unit.poststop
KillMode=none
Restart=on-failure
RestartSec=10s
TimeoutStartSec=200
TimeoutStopSec=120
StartLimitInterval=30min
StartLimitBurst=5
{extra_args}
[Install]
WantedBy=ceph-{fsid}.target
""".format(fsid=fsid,
           data_dir=ctx.data_dir,
           extra_args=extra_args,
           # if docker, we depend on docker.service
           docker_after=' docker.service' if docker else '',
           docker_requires='Requires=docker.service\n' if docker else '')

    return u

##################################


class CephContainer:
    def __init__(self,
                 ctx: CephadmContext,
                 image: str,
                 entrypoint: str,
                 args: List[str] = [],
                 volume_mounts: Dict[str, str] = {},
                 cname: str = '',
                 container_args: List[str] = [],
                 envs: Optional[List[str]] = None,
                 privileged: bool = False,
                 ptrace: bool = False,
                 bind_mounts: Optional[List[List[str]]] = None,
                 init: Optional[bool] = None,
                 host_network: bool = True,
                 memory_request: Optional[str] = None,
                 memory_limit: Optional[str] = None,
                 ) -> None:
        self.ctx = ctx
        self.image = image
        self.entrypoint = entrypoint
        self.args = args
        self.volume_mounts = volume_mounts
        self._cname = cname
        self.container_args = container_args
        self.envs = envs
        self.privileged = privileged
        self.ptrace = ptrace
        self.bind_mounts = bind_mounts if bind_mounts else []
        self.init = init if init else ctx.container_init
        self.host_network = host_network
        self.memory_request = memory_request
        self.memory_limit = memory_limit

    @classmethod
    def for_daemon(cls,
                   ctx: CephadmContext,
                   fsid: str,
                   daemon_type: str,
                   daemon_id: str,
                   entrypoint: str,
                   args: List[str] = [],
                   volume_mounts: Dict[str, str] = {},
                   container_args: List[str] = [],
                   envs: Optional[List[str]] = None,
                   privileged: bool = False,
                   ptrace: bool = False,
                   bind_mounts: Optional[List[List[str]]] = None,
                   init: Optional[bool] = None,
                   host_network: bool = True,
                   memory_request: Optional[str] = None,
                   memory_limit: Optional[str] = None,
                   ) -> 'CephContainer':
        return cls(
            ctx,
            image=ctx.image,
            entrypoint=entrypoint,
            args=args,
            volume_mounts=volume_mounts,
            cname='ceph-%s-%s.%s' % (fsid, daemon_type, daemon_id),
            container_args=container_args,
            envs=envs,
            privileged=privileged,
            ptrace=ptrace,
            bind_mounts=bind_mounts,
            init=init,
            host_network=host_network,
            memory_request=memory_request,
            memory_limit=memory_limit,
        )

    @property
    def cname(self) -> str:
        """
        podman adds the current container name to the /etc/hosts
        file. Turns out, python's `socket.getfqdn()` differs from
        `hostname -f`, when we have the container names containing
        dots in it.:

        # podman run --name foo.bar.baz.com ceph/ceph /bin/bash
        [root@sebastians-laptop /]# cat /etc/hosts
        127.0.0.1   localhost
        ::1         localhost
        127.0.1.1   sebastians-laptop foo.bar.baz.com
        [root@sebastians-laptop /]# hostname -f
        sebastians-laptop
        [root@sebastians-laptop /]# python3 -c 'import socket; print(socket.getfqdn())'
        foo.bar.baz.com

        Fascinatingly, this doesn't happen when using dashes.
        """
        return self._cname.replace('.', '-')

    @cname.setter
    def cname(self, val: str) -> None:
        self._cname = val

    @property
    def old_cname(self) -> str:
        return self._cname

    def run_cmd(self) -> List[str]:
        cmd_args: List[str] = [
            str(self.ctx.container_engine.path),
            'run',
            '--rm',
            '--ipc=host',
            # some containers (ahem, haproxy) override this, but we want a fast
            # shutdown always (and, more importantly, a successful exit even if we
            # fall back to SIGKILL).
            '--stop-signal=SIGTERM',
        ]

        if isinstance(self.ctx.container_engine, Podman):
            if os.path.exists('/etc/ceph/podman-auth.json'):
                cmd_args.append('--authfile=/etc/ceph/podman-auth.json')

        if isinstance(self.ctx.container_engine, Docker):
            cmd_args.extend(['--ulimit', 'nofile=1048576'])

        envs: List[str] = [
            '-e', 'CONTAINER_IMAGE=%s' % self.image,
            '-e', 'NODE_NAME=%s' % get_hostname(),
        ]
        vols: List[str] = []
        binds: List[str] = []

        if self.memory_request:
            cmd_args.extend(['-e', 'POD_MEMORY_REQUEST', str(self.memory_request)])
        if self.memory_limit:
            cmd_args.extend(['-e', 'POD_MEMORY_LIMIT', str(self.memory_limit)])
            cmd_args.extend(['--memory', str(self.memory_limit)])

        if self.host_network:
            cmd_args.append('--net=host')
        if self.entrypoint:
            cmd_args.extend(['--entrypoint', self.entrypoint])
        if self.privileged:
            cmd_args.extend([
                '--privileged',
                # let OSD etc read block devs that haven't been chowned
                '--group-add=disk'])
        if self.ptrace and not self.privileged:
            # if privileged, the SYS_PTRACE cap is already added
            # in addition, --cap-add and --privileged are mutually
            # exclusive since podman >= 2.0
            cmd_args.append('--cap-add=SYS_PTRACE')
        if self.init:
            cmd_args.append('--init')
            envs += ['-e', 'CEPH_USE_RANDOM_NONCE=1']
        if self.cname:
            cmd_args.extend(['--name', self.cname])
        if self.envs:
            for env in self.envs:
                envs.extend(['-e', env])

        vols = sum(
            [['-v', '%s:%s' % (host_dir, container_dir)]
             for host_dir, container_dir in self.volume_mounts.items()], [])
        binds = sum([['--mount', '{}'.format(','.join(bind))]
                     for bind in self.bind_mounts], [])

        return \
            cmd_args + self.container_args + \
            envs + vols + binds + \
            [self.image] + self.args  # type: ignore

    def shell_cmd(self, cmd: List[str]) -> List[str]:
        cmd_args: List[str] = [
            str(self.ctx.container_engine.path),
            'run',
            '--rm',
            '--ipc=host',
        ]
        envs: List[str] = [
            '-e', 'CONTAINER_IMAGE=%s' % self.image,
            '-e', 'NODE_NAME=%s' % get_hostname(),
        ]
        vols: List[str] = []
        binds: List[str] = []

        if self.host_network:
            cmd_args.append('--net=host')
        if self.ctx.no_hosts:
            cmd_args.append('--no-hosts')
        if self.privileged:
            cmd_args.extend([
                '--privileged',
                # let OSD etc read block devs that haven't been chowned
                '--group-add=disk',
            ])
        if self.init:
            cmd_args.append('--init')
            envs += ['-e', 'CEPH_USE_RANDOM_NONCE=1']
        if self.envs:
            for env in self.envs:
                envs.extend(['-e', env])

        vols = sum(
            [['-v', '%s:%s' % (host_dir, container_dir)]
             for host_dir, container_dir in self.volume_mounts.items()], [])
        binds = sum([['--mount', '{}'.format(','.join(bind))]
                     for bind in self.bind_mounts], [])

        return cmd_args + self.container_args + envs + vols + binds + [
            '--entrypoint', cmd[0],
            self.image,
        ] + cmd[1:]

    def exec_cmd(self, cmd):
        # type: (List[str]) -> List[str]
        cname = get_running_container_name(self.ctx, self)
        if not cname:
            raise Error('unable to find container "{}"'.format(self.cname))
        return [
            str(self.ctx.container_engine.path),
            'exec',
        ] + self.container_args + [
            self.cname,
        ] + cmd

    def rm_cmd(self, old_cname: bool = False, storage: bool = False) -> List[str]:
        ret = [
            str(self.ctx.container_engine.path),
            'rm', '-f',
        ]
        if storage:
            ret.append('--storage')
        if old_cname:
            ret.append(self.old_cname)
        else:
            ret.append(self.cname)
        return ret

    def stop_cmd(self, old_cname: bool = False, timeout: Optional[int] = None) -> List[str]:
        if timeout is None:
            ret = [
                str(self.ctx.container_engine.path),
                'stop', self.old_cname if old_cname else self.cname,
            ]
        else:
            ret = [
                str(self.ctx.container_engine.path),
                'stop', '-t', f'{timeout}',
                self.old_cname if old_cname else self.cname,
            ]
        return ret

    def run(self, timeout=DEFAULT_TIMEOUT, verbosity=CallVerbosity.VERBOSE_ON_FAILURE):
        # type: (Optional[int], CallVerbosity) -> str
        out, _, _ = call_throws(self.ctx, self.run_cmd(),
                                desc=self.entrypoint, timeout=timeout, verbosity=verbosity)
        return out


#####################################

class MgrListener(Thread):
    def __init__(self, agent: 'CephadmAgent') -> None:
        self.agent = agent
        self.stop = False
        super(MgrListener, self).__init__(target=self.run)

    def run(self) -> None:
        listenSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listenSocket.bind(('0.0.0.0', int(self.agent.listener_port)))
        listenSocket.settimeout(60)
        listenSocket.listen(1)
        ssl_ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        ssl_ctx.load_cert_chain(self.agent.listener_cert_path, self.agent.listener_key_path)
        ssl_ctx.load_verify_locations(self.agent.ca_path)
        secureListenSocket = ssl_ctx.wrap_socket(listenSocket, server_side=True)
        while not self.stop:
            try:
                try:
                    conn, _ = secureListenSocket.accept()
                except socket.timeout:
                    continue
                try:
                    length: int = int(conn.recv(10).decode())
                except Exception as e:
                    err_str = f'Failed to extract length of payload from message: {e}'
                    conn.send(err_str.encode())
                    logger.error(err_str)
                    continue
                while True:
                    payload = conn.recv(length).decode()
                    if not payload:
                        break
                    try:
                        data: Dict[Any, Any] = json.loads(payload)
                        self.handle_json_payload(data)
                    except Exception as e:
                        err_str = f'Failed to extract json payload from message: {e}'
                        conn.send(err_str.encode())
                        logger.error(err_str)
                    else:
                        conn.send(b'ACK')
                        if 'config' in data:
                            self.agent.wakeup()
                        self.agent.ls_gatherer.wakeup()
                        self.agent.volume_gatherer.wakeup()
                        logger.debug(f'Got mgr message {data}')
            except Exception as e:
                logger.error(f'Mgr Listener encountered exception: {e}')

    def shutdown(self) -> None:
        self.stop = True

    def handle_json_payload(self, data: Dict[Any, Any]) -> None:
        self.agent.ack = int(data['counter'])
        if 'config' in data:
            logger.info('Received new config from mgr')
            config = data['config']
            for filename in config:
                if filename in self.agent.required_files:
                    file_path = os.path.join(self.agent.daemon_dir, filename)
                    with write_new(file_path) as f:
                        f.write(config[filename])
            self.agent.pull_conf_settings()
            self.agent.wakeup()


class CephadmAgent():

    daemon_type = 'agent'
    default_port = 8498
    loop_interval = 30
    stop = False

    required_files = [
        'agent.json',
        'keyring',
        'root_cert.pem',
        'listener.crt',
        'listener.key',
    ]

    def __init__(self, ctx: CephadmContext, fsid: str, daemon_id: Union[int, str] = ''):
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.starting_port = 14873
        self.target_ip = ''
        self.target_port = ''
        self.host = ''
        self.daemon_dir = os.path.join(ctx.data_dir, self.fsid, f'{self.daemon_type}.{self.daemon_id}')
        self.config_path = os.path.join(self.daemon_dir, 'agent.json')
        self.keyring_path = os.path.join(self.daemon_dir, 'keyring')
        self.ca_path = os.path.join(self.daemon_dir, 'root_cert.pem')
        self.listener_cert_path = os.path.join(self.daemon_dir, 'listener.crt')
        self.listener_key_path = os.path.join(self.daemon_dir, 'listener.key')
        self.listener_port = ''
        self.ack = 1
        self.event = Event()
        self.mgr_listener = MgrListener(self)
        self.ls_gatherer = AgentGatherer(self, lambda: self._get_ls(), 'Ls')
        self.volume_gatherer = AgentGatherer(self, lambda: self._ceph_volume(enhanced=False), 'Volume')
        self.device_enhanced_scan = False
        self.recent_iteration_run_times: List[float] = [0.0, 0.0, 0.0]
        self.recent_iteration_index: int = 0
        self.cached_ls_values: Dict[str, Dict[str, str]] = {}

    def validate(self, config: Dict[str, str] = {}) -> None:
        # check for the required files
        for fname in self.required_files:
            if fname not in config:
                raise Error('required file missing from config: %s' % fname)

    def deploy_daemon_unit(self, config: Dict[str, str] = {}) -> None:
        if not config:
            raise Error('Agent needs a config')
        assert isinstance(config, dict)
        self.validate(config)

        # Create the required config files in the daemons dir, with restricted permissions
        for filename in config:
            if filename in self.required_files:
                file_path = os.path.join(self.daemon_dir, filename)
                with write_new(file_path) as f:
                    f.write(config[filename])

        unit_run_path = os.path.join(self.daemon_dir, 'unit.run')
        with write_new(unit_run_path) as f:
            f.write(self.unit_run())

        meta: Dict[str, Any] = fetch_meta(self.ctx)
        meta_file_path = os.path.join(self.daemon_dir, 'unit.meta')
        with write_new(meta_file_path) as f:
            f.write(json.dumps(meta, indent=4) + '\n')

        unit_file_path = os.path.join(self.ctx.unit_dir, self.unit_name())
        with write_new(unit_file_path) as f:
            f.write(self.unit_file())

        call_throws(self.ctx, ['systemctl', 'daemon-reload'])
        call(self.ctx, ['systemctl', 'stop', self.unit_name()],
             verbosity=CallVerbosity.DEBUG)
        call(self.ctx, ['systemctl', 'reset-failed', self.unit_name()],
             verbosity=CallVerbosity.DEBUG)
        call_throws(self.ctx, ['systemctl', 'enable', '--now', self.unit_name()])

    def unit_name(self) -> str:
        return '{}.service'.format(get_unit_name(self.fsid, self.daemon_type, self.daemon_id))

    def unit_run(self) -> str:
        py3 = shutil.which('python3')
        binary_path = os.path.realpath(sys.argv[0])
        return ('set -e\n' + f'{py3} {binary_path} agent --fsid {self.fsid} --daemon-id {self.daemon_id} &\n')

    def unit_file(self) -> str:
        return """#generated by cephadm
[Unit]
Description=cephadm agent for cluster {fsid}

PartOf=ceph-{fsid}.target
Before=ceph-{fsid}.target

[Service]
Type=forking
ExecStart=/bin/bash {data_dir}/unit.run
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=ceph-{fsid}.target
""".format(
            fsid=self.fsid,
            data_dir=self.daemon_dir
        )

    def shutdown(self) -> None:
        self.stop = True
        if self.mgr_listener.is_alive():
            self.mgr_listener.shutdown()
        if self.ls_gatherer.is_alive():
            self.ls_gatherer.shutdown()
        if self.volume_gatherer.is_alive():
            self.volume_gatherer.shutdown()

    def wakeup(self) -> None:
        self.event.set()

    def pull_conf_settings(self) -> None:
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
                self.target_ip = config['target_ip']
                self.target_port = config['target_port']
                self.loop_interval = int(config['refresh_period'])
                self.starting_port = int(config['listener_port'])
                self.host = config['host']
                use_lsm = config['device_enhanced_scan']
        except Exception as e:
            self.shutdown()
            raise Error(f'Failed to get agent target ip and port from config: {e}')

        try:
            with open(self.keyring_path, 'r') as f:
                self.keyring = f.read()
        except Exception as e:
            self.shutdown()
            raise Error(f'Failed to get agent keyring: {e}')

        assert self.target_ip and self.target_port

        self.device_enhanced_scan = False
        if use_lsm.lower() == 'true':
            self.device_enhanced_scan = True
        self.volume_gatherer.update_func(lambda: self._ceph_volume(enhanced=self.device_enhanced_scan))

    def query_endpoint(self,
                       addr: str = '',
                       port: str = '',
                       data: Optional[Union[Dict[str, str], str]] = None,
                       endpoint: str = '',
                       ssl_ctx: Optional[Any] = None) -> str:
        _addr = addr if addr else self.target_ip
        _port = port if port else self.target_port
        url = f'https://{_addr}:{_port}{endpoint}'

        try:
            req = Request(url, data, {'Content-Type': 'application/json'})
            send_time = time.monotonic()
            with urlopen(req, context=ssl_ctx) as response:
                response_str = response.read()
                response_json = json.loads(response_str)
                total_request_time = datetime.timedelta(seconds=(time.monotonic() - send_time)).total_seconds()
                logger.info(f'Received mgr response: "{response_json["result"]}" {total_request_time} seconds after sending request.')
        except Exception:
            raise
        return response_str

    def run(self) -> None:
        self.pull_conf_settings()

        t_node_proxy = Thread(target=cephadmlib.node_proxy.server.main)
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = True
        ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        ssl_ctx.load_verify_locations(self.ca_path)
        t_node_proxy.start()

        try:
            for _ in range(1001):
                if not port_in_use(self.ctx, EndPoint('0.0.0.0', self.starting_port)):
                    self.listener_port = str(self.starting_port)
                    break
                self.starting_port += 1
            if not self.listener_port:
                raise Error(f'All 1000 ports starting at {str(self.starting_port - 1001)} taken.')
        except Exception as e:
            raise Error(f'Failed to pick port for agent to listen on: {e}')

        if not self.mgr_listener.is_alive():
            self.mgr_listener.start()

        if not self.ls_gatherer.is_alive():
            self.ls_gatherer.start()

        if not self.volume_gatherer.is_alive():
            self.volume_gatherer.start()

        while not self.stop:
            start_time = time.monotonic()
            ack = self.ack

            # part of the networks info is returned as a set which is not JSON
            # serializable. The set must be converted to a list
            networks = list_networks(self.ctx)
            networks_list: Dict[str, Dict[str, List[str]]] = {}
            for key in networks.keys():
                networks_list[key] = {}
                for k, v in networks[key].items():
                    networks_list[key][k] = list(v)

            data = json.dumps({'host': self.host,
                               'ls': (self.ls_gatherer.data if self.ack == self.ls_gatherer.ack
                                      and self.ls_gatherer.data is not None else []),
                               'networks': networks_list,
                               'facts': HostFacts(self.ctx).dump(),
                               'volume': (self.volume_gatherer.data if self.ack == self.volume_gatherer.ack
                                          and self.volume_gatherer.data is not None else ''),
                               'ack': str(ack),
                               'keyring': self.keyring,
                               'port': self.listener_port})
            data = data.encode('ascii')

            try:
                self.query_endpoint(data=data,
                                    endpoint='/data/',
                                    ssl_ctx=ssl_ctx)
            except Exception as e:
                logger.error(f'Failed to send metadata to mgr: {e}')

            end_time = time.monotonic()
            run_time = datetime.timedelta(seconds=(end_time - start_time))
            self.recent_iteration_run_times[self.recent_iteration_index] = run_time.total_seconds()
            self.recent_iteration_index = (self.recent_iteration_index + 1) % 3
            run_time_average = sum(self.recent_iteration_run_times, 0.0) / len([t for t in self.recent_iteration_run_times if t])

            self.event.wait(max(self.loop_interval - int(run_time_average), 0))
            self.event.clear()

    def _ceph_volume(self, enhanced: bool = False) -> Tuple[str, bool]:
        self.ctx.command = 'inventory --format=json'.split()
        if enhanced:
            self.ctx.command.append('--with-lsm')
        self.ctx.fsid = self.fsid

        stream = io.StringIO()
        with redirect_stdout(stream):
            command_ceph_volume(self.ctx)

        stdout = stream.getvalue()

        if stdout:
            return (stdout, False)
        else:
            raise Exception('ceph-volume returned empty value')

    def _daemon_ls_subset(self) -> Dict[str, Dict[str, Any]]:
        # gets a subset of ls info quickly. The results of this will tell us if our
        # cached info is still good or if we need to run the full ls again.
        # for legacy containers, we just grab the full info. For cephadmv1 containers,
        # we only grab enabled, state, mem_usage and container id. If container id has
        # not changed for any daemon, we assume our cached info is good.
        daemons: Dict[str, Dict[str, Any]] = {}
        data_dir = self.ctx.data_dir
        seen_memusage = {}  # type: Dict[str, int]
        out, err, code = call(
            self.ctx,
            [self.ctx.container_engine.path, 'stats', '--format', '{{.ID}},{{.MemUsage}}', '--no-stream'],
            verbosity=CallVerbosity.DEBUG
        )
        seen_memusage_cid_len, seen_memusage = _parse_mem_usage(code, out)
        # we need a mapping from container names to ids. Later we will convert daemon
        # names to container names to get daemons container id to see if it has changed
        out, err, code = call(
            self.ctx,
            [self.ctx.container_engine.path, 'ps', '--format', '{{.ID}},{{.Names}}', '--no-trunc'],
            verbosity=CallVerbosity.DEBUG
        )
        name_id_mapping: Dict[str, str] = self._parse_container_id_name(code, out)
        for i in os.listdir(data_dir):
            if i in ['mon', 'osd', 'mds', 'mgr']:
                daemon_type = i
                for j in os.listdir(os.path.join(data_dir, i)):
                    if '-' not in j:
                        continue
                    (cluster, daemon_id) = j.split('-', 1)
                    legacy_unit_name = 'ceph-%s@%s' % (daemon_type, daemon_id)
                    (enabled, state, _) = check_unit(self.ctx, legacy_unit_name)
                    daemons[f'{daemon_type}.{daemon_id}'] = {
                        'style': 'legacy',
                        'name': '%s.%s' % (daemon_type, daemon_id),
                        'fsid': self.ctx.fsid if self.ctx.fsid is not None else 'unknown',
                        'systemd_unit': legacy_unit_name,
                        'enabled': 'true' if enabled else 'false',
                        'state': state,
                    }
            elif is_fsid(i):
                fsid = str(i)  # convince mypy that fsid is a str here
                for j in os.listdir(os.path.join(data_dir, i)):
                    if '.' in j and os.path.isdir(os.path.join(data_dir, fsid, j)):
                        (daemon_type, daemon_id) = j.split('.', 1)
                        unit_name = get_unit_name(fsid, daemon_type, daemon_id)
                        (enabled, state, _) = check_unit(self.ctx, unit_name)
                        daemons[j] = {
                            'style': 'cephadm:v1',
                            'systemd_unit': unit_name,
                            'enabled': 'true' if enabled else 'false',
                            'state': state,
                        }
                        c = CephContainer.for_daemon(self.ctx, self.ctx.fsid, daemon_type, daemon_id, 'bash')
                        container_id: Optional[str] = None
                        for name in (c.cname, c.old_cname):
                            if name in name_id_mapping:
                                container_id = name_id_mapping[name]
                                break
                        daemons[j]['container_id'] = container_id
                        if container_id:
                            daemons[j]['memory_usage'] = seen_memusage.get(container_id[0:seen_memusage_cid_len])
        return daemons

    def _parse_container_id_name(self, code: int, out: str) -> Dict[str, str]:
        # map container names to ids from ps output
        name_id_mapping = {}  # type: Dict[str, str]
        if not code:
            for line in out.splitlines():
                id, name = line.split(',')
                name_id_mapping[name] = id
        return name_id_mapping

    def _get_ls(self) -> Tuple[List[Dict[str, str]], bool]:
        if not self.cached_ls_values:
            logger.info('No cached ls output. Running full daemon ls')
            ls = list_daemons(self.ctx)
            for d in ls:
                self.cached_ls_values[d['name']] = d
            return (ls, True)
        else:
            ls_subset = self._daemon_ls_subset()
            need_full_ls = False
            state_change = False
            if set(self.cached_ls_values.keys()) != set(ls_subset.keys()):
                # case for a new daemon in ls or an old daemon no longer appearing.
                # If that happens we need a full ls
                logger.info('Change detected in state of daemons. Running full daemon ls')
                self.cached_ls_values = {}
                ls = list_daemons(self.ctx)
                for d in ls:
                    self.cached_ls_values[d['name']] = d
                return (ls, True)
            for daemon, info in self.cached_ls_values.items():
                if info['style'] == 'legacy':
                    # for legacy containers, ls_subset just grabs all the info
                    self.cached_ls_values[daemon] = ls_subset[daemon]
                else:
                    if info['container_id'] != ls_subset[daemon]['container_id']:
                        # case for container id having changed. We need full ls as
                        # info we didn't grab like version and start time could have changed
                        need_full_ls = True
                        break

                    # want to know if a daemons state change because in those cases we want
                    # to report back quicker
                    if (
                        self.cached_ls_values[daemon]['enabled'] != ls_subset[daemon]['enabled']
                        or self.cached_ls_values[daemon]['state'] != ls_subset[daemon]['state']
                    ):
                        state_change = True
                    # if we reach here, container id matched. Update the few values we do track
                    # from ls subset: state, enabled, memory_usage.
                    self.cached_ls_values[daemon]['enabled'] = ls_subset[daemon]['enabled']
                    self.cached_ls_values[daemon]['state'] = ls_subset[daemon]['state']
                    if 'memory_usage' in ls_subset[daemon]:
                        self.cached_ls_values[daemon]['memory_usage'] = ls_subset[daemon]['memory_usage']
            if need_full_ls:
                logger.info('Change detected in state of daemons. Running full daemon ls')
                ls = list_daemons(self.ctx)
                self.cached_ls_values = {}
                for d in ls:
                    self.cached_ls_values[d['name']] = d
                return (ls, True)
            else:
                ls = [info for daemon, info in self.cached_ls_values.items()]
                return (ls, state_change)


class AgentGatherer(Thread):
    def __init__(self, agent: 'CephadmAgent', func: Callable, gatherer_type: str = 'Unnamed', initial_ack: int = 0) -> None:
        self.agent = agent
        self.func = func
        self.gatherer_type = gatherer_type
        self.ack = initial_ack
        self.event = Event()
        self.data: Any = None
        self.stop = False
        self.recent_iteration_run_times: List[float] = [0.0, 0.0, 0.0]
        self.recent_iteration_index: int = 0
        super(AgentGatherer, self).__init__(target=self.run)

    def run(self) -> None:
        while not self.stop:
            try:
                start_time = time.monotonic()

                ack = self.agent.ack
                change = False
                try:
                    self.data, change = self.func()
                except Exception as e:
                    logger.error(f'{self.gatherer_type} Gatherer encountered exception gathering data: {e}')
                    self.data = None
                if ack != self.ack or change:
                    self.ack = ack
                    self.agent.wakeup()

                end_time = time.monotonic()
                run_time = datetime.timedelta(seconds=(end_time - start_time))
                self.recent_iteration_run_times[self.recent_iteration_index] = run_time.total_seconds()
                self.recent_iteration_index = (self.recent_iteration_index + 1) % 3
                run_time_average = sum(self.recent_iteration_run_times, 0.0) / len([t for t in self.recent_iteration_run_times if t])

                self.event.wait(max(self.agent.loop_interval - int(run_time_average), 0))
                self.event.clear()
            except Exception as e:
                logger.error(f'{self.gatherer_type} Gatherer encountered exception: {e}')

    def shutdown(self) -> None:
        self.stop = True

    def wakeup(self) -> None:
        self.event.set()

    def update_func(self, func: Callable) -> None:
        self.func = func


def command_agent(ctx: CephadmContext) -> None:
    agent = CephadmAgent(ctx, ctx.fsid, ctx.daemon_id)

    if not os.path.isdir(agent.daemon_dir):
        raise Error(f'Agent daemon directory {agent.daemon_dir} does not exist. Perhaps agent was never deployed?')

    agent.run()


##################################

@executes_early
def command_version(ctx):
    # type: (CephadmContext) -> int
    import importlib

    try:
        vmod = importlib.import_module('_version')
    except ImportError:
        print('cephadm version UNKNOWN')
        return 1
    _unset = '<UNSET>'
    print('cephadm version {0} ({1}) {2} ({3})'.format(
        getattr(vmod, 'CEPH_GIT_NICE_VER', _unset),
        getattr(vmod, 'CEPH_GIT_VER', _unset),
        getattr(vmod, 'CEPH_RELEASE_NAME', _unset),
        getattr(vmod, 'CEPH_RELEASE_TYPE', _unset),
    ))
    return 0

##################################


@default_image
def command_pull(ctx):
    # type: (CephadmContext) -> int

    try:
        _pull_image(ctx, ctx.image, ctx.insecure)
    except UnauthorizedRegistryError:
        err_str = 'Failed to pull container image. Check that host(s) are logged into the registry'
        logger.debug(f'Pulling image for `command_pull` failed: {err_str}')
        raise Error(err_str)
    return command_inspect_image(ctx)


def _pull_image(ctx, image, insecure=False):
    # type: (CephadmContext, str, bool) -> None
    logger.info('Pulling container image %s...' % image)

    ignorelist = [
        'error creating read-write layer with ID',
        'net/http: TLS handshake timeout',
        'Digest did not match, expected',
    ]

    cmd = [ctx.container_engine.path, 'pull', image]
    if isinstance(ctx.container_engine, Podman):
        if insecure:
            cmd.append('--tls-verify=false')

        if os.path.exists('/etc/ceph/podman-auth.json'):
            cmd.append('--authfile=/etc/ceph/podman-auth.json')
    cmd_str = ' '.join(cmd)

    for sleep_secs in [1, 4, 25]:
        out, err, ret = call(ctx, cmd, verbosity=CallVerbosity.QUIET_UNLESS_ERROR)
        if not ret:
            return

        if 'unauthorized' in err:
            raise UnauthorizedRegistryError()

        if not any(pattern in err for pattern in ignorelist):
            raise Error('Failed command: %s' % cmd_str)

        logger.info('`%s` failed transiently. Retrying. waiting %s seconds...' % (cmd_str, sleep_secs))
        time.sleep(sleep_secs)

    raise Error('Failed command: %s: maximum retries reached' % cmd_str)

##################################


@require_image
@infer_image
def command_inspect_image(ctx):
    # type: (CephadmContext) -> int
    out, err, ret = call_throws(ctx, [
        ctx.container_engine.path, 'inspect',
        '--format', '{{.ID}},{{.RepoDigests}}',
        ctx.image])
    if ret:
        return errno.ENOENT
    info_from = get_image_info_from_inspect(out.strip(), ctx.image)

    ver = CephContainer(ctx, ctx.image, 'ceph', ['--version']).run().strip()
    info_from['ceph_version'] = ver

    print(json.dumps(info_from, indent=4, sort_keys=True))
    return 0


def normalize_image_digest(digest: str) -> str:
    """
    Normal case:
    >>> normalize_image_digest('ceph/ceph', 'docker.io')
    'docker.io/ceph/ceph'

    No change:
    >>> normalize_image_digest('quay.ceph.io/ceph/ceph', 'docker.io')
    'quay.ceph.io/ceph/ceph'

    >>> normalize_image_digest('docker.io/ubuntu', 'docker.io')
    'docker.io/ubuntu'

    >>> normalize_image_digest('localhost/ceph', 'docker.io')
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


def get_image_info_from_inspect(out, image):
    # type: (str, str) -> Dict[str, Union[str,List[str]]]
    image_id, digests = out.split(',', 1)
    if not out:
        raise Error('inspect {}: empty result'.format(image))
    r = {
        'image_id': normalize_container_id(image_id)
    }  # type: Dict[str, Union[str,List[str]]]
    if digests:
        r['repo_digests'] = list(map(normalize_image_digest, digests[1: -1].split(' ')))
    return r

##################################


def check_subnet(subnets: str) -> Tuple[int, List[int], str]:
    """Determine whether the given string is a valid subnet

    :param subnets: subnet string, a single definition or comma separated list of CIDR subnets
    :returns: return code, IP version list of the subnets and msg describing any errors validation errors
    """

    rc = 0
    versions = set()
    errors = []
    subnet_list = subnets.split(',')
    for subnet in subnet_list:
        # ensure the format of the string is as expected address/netmask
        subnet = subnet.strip()
        if not re.search(r'\/\d+$', subnet):
            rc = 1
            errors.append(f'{subnet} is not in CIDR format (address/netmask)')
            continue
        try:
            v = ipaddress.ip_network(subnet).version
            versions.add(v)
        except ValueError as e:
            rc = 1
            errors.append(f'{subnet} invalid: {str(e)}')

    return rc, list(versions), ', '.join(errors)


def unwrap_ipv6(address):
    # type: (str) -> str
    if address.startswith('[') and address.endswith(']'):
        return address[1: -1]
    return address


def wrap_ipv6(address):
    # type: (str) -> str

    # We cannot assume it's already wrapped or even an IPv6 address if
    # it's already wrapped it'll not pass (like if it's a hostname) and trigger
    # the ValueError
    try:
        if ipaddress.ip_address(address).version == 6:
            return f'[{address}]'
    except ValueError:
        pass

    return address


def is_ipv6(address):
    # type: (str) -> bool
    address = unwrap_ipv6(address)
    try:
        return ipaddress.ip_address(address).version == 6
    except ValueError:
        logger.warning('Address: {} is not a valid IP address'.format(address))
        return False


def ip_in_subnets(ip_addr: str, subnets: str) -> bool:
    """Determine if the ip_addr belongs to any of the subnets list."""
    subnet_list = [x.strip() for x in subnets.split(',')]
    for subnet in subnet_list:
        ip_address = unwrap_ipv6(ip_addr) if is_ipv6(ip_addr) else ip_addr
        if ipaddress.ip_address(ip_address) in ipaddress.ip_network(subnet):
            return True
    return False


def parse_mon_addrv(addrv_arg: str) -> List[EndPoint]:
    """Parse mon-addrv param into a list of mon end points."""
    r = re.compile(r':(\d+)$')
    addrv_args = []
    addr_arg = addrv_arg
    if addr_arg[0] != '[' or addr_arg[-1] != ']':
        raise Error(f'--mon-addrv value {addr_arg} must use square brackets')

    for addr in addr_arg[1: -1].split(','):
        hasport = r.findall(addr)
        if not hasport:
            raise Error(f'--mon-addrv value {addr_arg} must include port number')
        port_str = hasport[0]
        addr = re.sub(r'^v\d+:', '', addr)  # strip off v1: or v2: prefix
        base_ip = addr[0:-(len(port_str)) - 1]
        addrv_args.append(EndPoint(base_ip, int(port_str)))

    return addrv_args


def parse_mon_ip(mon_ip: str) -> List[EndPoint]:
    """Parse mon-ip param into a list of mon end points."""
    r = re.compile(r':(\d+)$')
    addrv_args = []
    hasport = r.findall(mon_ip)
    if hasport:
        port_str = hasport[0]
        base_ip = mon_ip[0:-(len(port_str)) - 1]
        addrv_args.append(EndPoint(base_ip, int(port_str)))
    else:
        # No port provided: use fixed ports for ceph monitor
        addrv_args.append(EndPoint(mon_ip, 3300))
        addrv_args.append(EndPoint(mon_ip, 6789))

    return addrv_args


def build_addrv_params(addrv: List[EndPoint]) -> str:
    """Convert mon end-points (ip:port) into the format: [v[1|2]:ip:port1]"""
    if len(addrv) > 2:
        raise Error('Detected a local mon-addrv list with more than 2 entries.')
    port_to_ver: Dict[int, str] = {6789: 'v1', 3300: 'v2'}
    addr_arg_list: List[str] = []
    for ep in addrv:
        if ep.port in port_to_ver:
            ver = port_to_ver[ep.port]
        else:
            ver = 'v2'  # default mon protocol version if port is not provided
            logger.warning(f'Using msgr2 protocol for unrecognized port {ep}')
        addr_arg_list.append(f'{ver}:{ep.ip}:{ep.port}')

    addr_arg = '[{0}]'.format(','.join(addr_arg_list))
    return addr_arg


def get_public_net_from_cfg(ctx: CephadmContext) -> Optional[str]:
    """Get mon public network from configuration file."""
    cp = read_config(ctx.config)
    if not cp.has_option('global', 'public_network'):
        return None

    # Ensure all public CIDR networks are valid
    public_network = cp.get('global', 'public_network').strip('"').strip("'")
    rc, _, err_msg = check_subnet(public_network)
    if rc:
        raise Error(f'Invalid public_network {public_network} parameter: {err_msg}')

    # Ensure all public CIDR networks are configured locally
    configured_subnets = set([x.strip() for x in public_network.split(',')])
    local_subnets = set([x[0] for x in list_networks(ctx).items()])
    valid_public_net = False
    for net in configured_subnets:
        if net in local_subnets:
            valid_public_net = True
        else:
            logger.warning(f'The public CIDR network {net} (from -c conf file) is not configured locally.')
    if not valid_public_net:
        raise Error(f'None of the public CIDR network(s) {configured_subnets} (from -c conf file) is configured locally.')

    # Ensure public_network is compatible with the provided mon-ip (or mon-addrv)
    if ctx.mon_ip:
        if not ip_in_subnets(ctx.mon_ip, public_network):
            raise Error(f'The provided --mon-ip {ctx.mon_ip} does not belong to any public_network(s) {public_network}')
    elif ctx.mon_addrv:
        addrv_args = parse_mon_addrv(ctx.mon_addrv)
        for addrv in addrv_args:
            if not ip_in_subnets(addrv.ip, public_network):
                raise Error(f'The provided --mon-addrv {addrv.ip} ip does not belong to any public_network(s) {public_network}')

    logger.debug(f'Using mon public network from configuration file {public_network}')
    return public_network


def infer_mon_network(ctx: CephadmContext, mon_eps: List[EndPoint]) -> Optional[str]:
    """Infer mon public network from local network."""
    # Make sure IP is configured locally, and then figure out the CIDR network
    mon_networks = []
    for net, ifaces in list_networks(ctx).items():
        # build local_ips list for the specified network
        local_ips: List[Union[ipaddress.IPv4Address, ipaddress.IPv6Address]] = []
        for _, ls in ifaces.items():
            local_ips.extend([ipaddress.ip_address(ip) for ip in ls])

        # check if any of mon ips belong to this net
        for mon_ep in mon_eps:
            try:
                if ipaddress.ip_address(unwrap_ipv6(mon_ep.ip)) in local_ips:
                    mon_networks.append(net)
                    logger.info(f'Mon IP `{mon_ep.ip}` is in CIDR network `{net}`')
            except ValueError as e:
                logger.warning(f'Cannot infer CIDR network for mon IP `{mon_ep.ip}` : {e}')

    if not mon_networks:
        raise Error('Cannot infer CIDR network. Pass --skip-mon-network to configure it later')
    else:
        logger.debug(f'Inferred mon public CIDR from local network configuration {mon_networks}')

    mon_networks = list(set(mon_networks))  # remove duplicates
    return ','.join(mon_networks)


def prepare_mon_addresses(ctx: CephadmContext) -> Tuple[str, bool, Optional[str]]:
    """Get mon public network configuration."""
    ipv6 = False
    addrv_args: List[EndPoint] = []
    mon_addrv: str = ''  # i.e: [v2:192.168.100.1:3300,v1:192.168.100.1:6789]

    if ctx.mon_ip:
        ipv6 = is_ipv6(ctx.mon_ip)
        if ipv6:
            ctx.mon_ip = wrap_ipv6(ctx.mon_ip)
        addrv_args = parse_mon_ip(ctx.mon_ip)
        mon_addrv = build_addrv_params(addrv_args)
    elif ctx.mon_addrv:
        ipv6 = ctx.mon_addrv.count('[') > 1
        addrv_args = parse_mon_addrv(ctx.mon_addrv)
        mon_addrv = ctx.mon_addrv
    else:
        raise Error('must specify --mon-ip or --mon-addrv')

    if addrv_args:
        for end_point in addrv_args:
            check_ip_port(ctx, end_point)

    logger.debug(f'Base mon IP(s) is {addrv_args}, mon addrv is {mon_addrv}')
    mon_network = None
    if not ctx.skip_mon_network:
        mon_network = get_public_net_from_cfg(ctx) or infer_mon_network(ctx, addrv_args)

    return (mon_addrv, ipv6, mon_network)


def prepare_cluster_network(ctx: CephadmContext) -> Tuple[str, bool]:
    # the cluster network may not exist on this node, so all we can do is
    # validate that the address given is valid ipv4 or ipv6 subnet
    ipv6_cluster_network = False
    cp = read_config(ctx.config)
    cluster_network = ctx.cluster_network
    if cluster_network is None and cp.has_option('global', 'cluster_network'):
        cluster_network = cp.get('global', 'cluster_network').strip('"').strip("'")

    if cluster_network:
        cluster_nets = set([x.strip() for x in cluster_network.split(',')])
        local_subnets = set([x[0] for x in list_networks(ctx).items()])
        for net in cluster_nets:
            if net not in local_subnets:
                logger.warning(f'The cluster CIDR network {net} is not configured locally.')

        rc, versions, err_msg = check_subnet(cluster_network)
        if rc:
            raise Error(f'Invalid --cluster-network parameter: {err_msg}')
        ipv6_cluster_network = True if 6 in versions else False
    else:
        logger.info('Internal network (--cluster-network) has not '
                    'been provided, OSD replication will default to '
                    'the public_network')

    return cluster_network, ipv6_cluster_network


def create_initial_keys(
    ctx: CephadmContext,
    uid: int, gid: int,
    mgr_id: str
) -> Tuple[str, str, str, Any, Any]:  # type: ignore

    _image = ctx.image

    # create some initial keys
    logger.info('Creating initial keys...')
    mon_key = CephContainer(
        ctx,
        image=_image,
        entrypoint='/usr/bin/ceph-authtool',
        args=['--gen-print-key'],
    ).run().strip()
    admin_key = CephContainer(
        ctx,
        image=_image,
        entrypoint='/usr/bin/ceph-authtool',
        args=['--gen-print-key'],
    ).run().strip()
    mgr_key = CephContainer(
        ctx,
        image=_image,
        entrypoint='/usr/bin/ceph-authtool',
        args=['--gen-print-key'],
    ).run().strip()

    keyring = ('[mon.]\n'
               '\tkey = %s\n'
               '\tcaps mon = allow *\n'
               '[client.admin]\n'
               '\tkey = %s\n'
               '\tcaps mon = allow *\n'
               '\tcaps mds = allow *\n'
               '\tcaps mgr = allow *\n'
               '\tcaps osd = allow *\n'
               '[mgr.%s]\n'
               '\tkey = %s\n'
               '\tcaps mon = profile mgr\n'
               '\tcaps mds = allow *\n'
               '\tcaps osd = allow *\n'
               % (mon_key, admin_key, mgr_id, mgr_key))

    admin_keyring = write_tmp('[client.admin]\n'
                              '\tkey = ' + admin_key + '\n',
                              uid, gid)

    # tmp keyring file
    bootstrap_keyring = write_tmp(keyring, uid, gid)
    return (mon_key, mgr_key, admin_key,
            bootstrap_keyring, admin_keyring)


def create_initial_monmap(
    ctx: CephadmContext,
    uid: int, gid: int,
    fsid: str,
    mon_id: str, mon_addr: str
) -> Any:
    logger.info('Creating initial monmap...')
    monmap = write_tmp('', 0, 0)
    out = CephContainer(
        ctx,
        image=ctx.image,
        entrypoint='/usr/bin/monmaptool',
        args=[
            '--create',
            '--clobber',
            '--fsid', fsid,
            '--addv', mon_id, mon_addr,
            '/tmp/monmap'
        ],
        volume_mounts={
            monmap.name: '/tmp/monmap:z',
        },
    ).run()
    logger.debug(f'monmaptool for {mon_id} {mon_addr} on {out}')

    # pass monmap file to ceph user for use by ceph-mon --mkfs below
    os.fchown(monmap.fileno(), uid, gid)
    return monmap


def prepare_create_mon(
    ctx: CephadmContext,
    uid: int, gid: int,
    fsid: str, mon_id: str,
    bootstrap_keyring_path: str,
    monmap_path: str
) -> Tuple[str, str]:
    logger.info('Creating mon...')
    create_daemon_dirs(ctx, fsid, 'mon', mon_id, uid, gid)
    mon_dir = get_data_dir(fsid, ctx.data_dir, 'mon', mon_id)
    log_dir = get_log_dir(fsid, ctx.log_dir)
    out = CephContainer(
        ctx,
        image=ctx.image,
        entrypoint='/usr/bin/ceph-mon',
        args=[
            '--mkfs',
            '-i', mon_id,
            '--fsid', fsid,
            '-c', '/dev/null',
            '--monmap', '/tmp/monmap',
            '--keyring', '/tmp/keyring',
        ] + get_daemon_args(ctx, fsid, 'mon', mon_id),
        volume_mounts={
            log_dir: '/var/log/ceph:z',
            mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % (mon_id),
            bootstrap_keyring_path: '/tmp/keyring:z',
            monmap_path: '/tmp/monmap:z',
        },
    ).run()
    logger.debug(f'create mon.{mon_id} on {out}')
    return (mon_dir, log_dir)


def create_mon(
    ctx: CephadmContext,
    uid: int, gid: int,
    fsid: str, mon_id: str
) -> None:
    mon_c = get_container(ctx, fsid, 'mon', mon_id)
    ctx.meta_properties = {'service_name': 'mon'}
    deploy_daemon(ctx, fsid, 'mon', mon_id, mon_c, uid, gid,
                  config=None, keyring=None)


def wait_for_mon(
    ctx: CephadmContext,
    mon_id: str, mon_dir: str,
    admin_keyring_path: str, config_path: str
) -> None:
    logger.info('Waiting for mon to start...')
    c = CephContainer(
        ctx,
        image=ctx.image,
        entrypoint='/usr/bin/ceph',
        args=[
            'status'],
        volume_mounts={
            mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % (mon_id),
            admin_keyring_path: '/etc/ceph/ceph.client.admin.keyring:z',
            config_path: '/etc/ceph/ceph.conf:z',
        },
    )

    # wait for the service to become available
    def is_mon_available():
        # type: () -> bool
        timeout = ctx.timeout if ctx.timeout else 60  # seconds
        out, err, ret = call(ctx, c.run_cmd(),
                             desc=c.entrypoint,
                             timeout=timeout,
                             verbosity=CallVerbosity.QUIET_UNLESS_ERROR)
        return ret == 0

    is_available(ctx, 'mon', is_mon_available)


def create_mgr(
    ctx: CephadmContext,
    uid: int, gid: int,
    fsid: str, mgr_id: str, mgr_key: str,
    config: str, clifunc: Callable
) -> None:
    logger.info('Creating mgr...')
    mgr_keyring = '[mgr.%s]\n\tkey = %s\n' % (mgr_id, mgr_key)
    mgr_c = get_container(ctx, fsid, 'mgr', mgr_id)
    # Note:the default port used by the Prometheus node exporter is opened in fw
    ctx.meta_properties = {'service_name': 'mgr'}
    endpoints = [EndPoint('0.0.0.0', 9283), EndPoint('0.0.0.0', 8765)]
    if not ctx.skip_monitoring_stack:
        endpoints.append(EndPoint('0.0.0.0', 8443))
    deploy_daemon(ctx, fsid, 'mgr', mgr_id, mgr_c, uid, gid,
                  config=config, keyring=mgr_keyring, endpoints=endpoints)

    # wait for the service to become available
    logger.info('Waiting for mgr to start...')

    def is_mgr_available():
        # type: () -> bool
        timeout = ctx.timeout if ctx.timeout else 60  # seconds
        try:
            out = clifunc(['status', '-f', 'json-pretty'],
                          timeout=timeout,
                          verbosity=CallVerbosity.QUIET_UNLESS_ERROR)
            j = json.loads(out)
            return j.get('mgrmap', {}).get('available', False)
        except Exception as e:
            logger.debug('status failed: %s' % e)
            return False

    is_available(ctx, 'mgr', is_mgr_available)


def prepare_ssh(
    ctx: CephadmContext,
    cli: Callable, wait_for_mgr_restart: Callable
) -> None:

    cli(['cephadm', 'set-user', ctx.ssh_user])

    if ctx.ssh_config:
        logger.info('Using provided ssh config...')
        mounts = {
            pathify(ctx.ssh_config.name): '/tmp/cephadm-ssh-config:z',
        }
        cli(['cephadm', 'set-ssh-config', '-i', '/tmp/cephadm-ssh-config'], extra_mounts=mounts)

    if ctx.ssh_private_key and ctx.ssh_public_key:
        logger.info('Using provided ssh keys...')
        mounts = {
            pathify(ctx.ssh_private_key.name): '/tmp/cephadm-ssh-key:z',
            pathify(ctx.ssh_public_key.name): '/tmp/cephadm-ssh-key.pub:z'
        }
        cli(['cephadm', 'set-priv-key', '-i', '/tmp/cephadm-ssh-key'], extra_mounts=mounts)
        cli(['cephadm', 'set-pub-key', '-i', '/tmp/cephadm-ssh-key.pub'], extra_mounts=mounts)
        ssh_pub = cli(['cephadm', 'get-pub-key'])
        authorize_ssh_key(ssh_pub, ctx.ssh_user)
    elif ctx.ssh_private_key and ctx.ssh_signed_cert:
        logger.info('Using provided ssh private key and signed cert ...')
        mounts = {
            pathify(ctx.ssh_private_key.name): '/tmp/cephadm-ssh-key:z',
            pathify(ctx.ssh_signed_cert.name): '/tmp/cephadm-ssh-key-cert.pub:z'
        }
        cli(['cephadm', 'set-priv-key', '-i', '/tmp/cephadm-ssh-key'], extra_mounts=mounts)
        cli(['cephadm', 'set-signed-cert', '-i', '/tmp/cephadm-ssh-key-cert.pub'], extra_mounts=mounts)
    else:
        logger.info('Generating ssh key...')
        cli(['cephadm', 'generate-key'])
        ssh_pub = cli(['cephadm', 'get-pub-key'])
        with open(ctx.output_pub_ssh_key, 'w') as f:
            f.write(ssh_pub)
        logger.info('Wrote public SSH key to %s' % ctx.output_pub_ssh_key)
        authorize_ssh_key(ssh_pub, ctx.ssh_user)

    host = get_hostname()
    logger.info('Adding host %s...' % host)
    try:
        args = ['orch', 'host', 'add', host]
        if ctx.mon_ip:
            args.append(unwrap_ipv6(ctx.mon_ip))
        elif ctx.mon_addrv:
            addrv_args = parse_mon_addrv(ctx.mon_addrv)
            args.append(unwrap_ipv6(addrv_args[0].ip))
        cli(args)
    except RuntimeError as e:
        raise Error('Failed to add host <%s>: %s' % (host, e))

    for t in ['mon', 'mgr']:
        if not ctx.orphan_initial_daemons:
            logger.info('Deploying %s service with default placement...' % t)
            cli(['orch', 'apply', t])
        else:
            logger.info('Deploying unmanaged %s service...' % t)
            cli(['orch', 'apply', t, '--unmanaged'])

    if not ctx.orphan_initial_daemons:
        logger.info('Deploying crash service with default placement...')
        cli(['orch', 'apply', 'crash'])

    if not ctx.skip_monitoring_stack:
        for t in ['ceph-exporter', 'prometheus', 'grafana', 'node-exporter', 'alertmanager']:
            logger.info('Deploying %s service with default placement...' % t)
            try:
                cli(['orch', 'apply', t])
            except RuntimeError:
                ctx.error_code = -errno.EINVAL
                logger.error(f'Failed to apply service type {t}. '
                             'Perhaps the ceph version being bootstrapped does not support it')

    if ctx.with_centralized_logging:
        for t in ['loki', 'promtail']:
            logger.info('Deploying %s service with default placement...' % t)
            try:
                cli(['orch', 'apply', t])
            except RuntimeError:
                ctx.error_code = -errno.EINVAL
                logger.error(f'Failed to apply service type {t}. '
                             'Perhaps the ceph version being bootstrapped does not support it')


def enable_cephadm_mgr_module(
    cli: Callable, wait_for_mgr_restart: Callable
) -> None:

    logger.info('Enabling cephadm module...')
    cli(['mgr', 'module', 'enable', 'cephadm'])
    wait_for_mgr_restart()
    logger.info('Setting orchestrator backend to cephadm...')
    cli(['orch', 'set', 'backend', 'cephadm'])


def prepare_dashboard(
    ctx: CephadmContext,
    uid: int, gid: int,
    cli: Callable, wait_for_mgr_restart: Callable
) -> None:

    # Configure SSL port (cephadm only allows to configure dashboard SSL port)
    # if the user does not want to use SSL he can change this setting once the cluster is up
    cli(['config', 'set', 'mgr', 'mgr/dashboard/ssl_server_port', str(ctx.ssl_dashboard_port)])

    # configuring dashboard parameters
    logger.info('Enabling the dashboard module...')
    cli(['mgr', 'module', 'enable', 'dashboard'])
    wait_for_mgr_restart()

    # dashboard crt and key
    if ctx.dashboard_key and ctx.dashboard_crt:
        logger.info('Using provided dashboard certificate...')
        mounts = {
            pathify(ctx.dashboard_crt.name): '/tmp/dashboard.crt:z',
            pathify(ctx.dashboard_key.name): '/tmp/dashboard.key:z'
        }
        cli(['dashboard', 'set-ssl-certificate', '-i', '/tmp/dashboard.crt'], extra_mounts=mounts)
        cli(['dashboard', 'set-ssl-certificate-key', '-i', '/tmp/dashboard.key'], extra_mounts=mounts)
    else:
        logger.info('Generating a dashboard self-signed certificate...')
        cli(['dashboard', 'create-self-signed-cert'])

    logger.info('Creating initial admin user...')
    password = ctx.initial_dashboard_password or generate_password()
    tmp_password_file = write_tmp(password, uid, gid)
    cmd = ['dashboard', 'ac-user-create', ctx.initial_dashboard_user, '-i', '/tmp/dashboard.pw', 'administrator', '--force-password']
    if not ctx.dashboard_password_noupdate:
        cmd.append('--pwd-update-required')
    cli(cmd, extra_mounts={pathify(tmp_password_file.name): '/tmp/dashboard.pw:z'})
    logger.info('Fetching dashboard port number...')
    out = cli(['config', 'get', 'mgr', 'mgr/dashboard/ssl_server_port'])
    port = int(out)

    # Open dashboard port
    if not ('skip_firewalld' in ctx and ctx.skip_firewalld):
        fw = Firewalld(ctx)
        fw.open_ports([port])
        fw.apply_rules()

    logger.info('Ceph Dashboard is now available at:\n\n'
                '\t     URL: https://%s:%s/\n'
                '\t    User: %s\n'
                '\tPassword: %s\n' % (
                    get_fqdn(), port,
                    ctx.initial_dashboard_user,
                    password))


def prepare_bootstrap_config(
    ctx: CephadmContext,
    fsid: str, mon_addr: str, image: str

) -> str:

    cp = read_config(ctx.config)
    if not cp.has_section('global'):
        cp.add_section('global')
    cp.set('global', 'fsid', fsid)
    cp.set('global', 'mon_host', mon_addr)
    cp.set('global', 'container_image', image)

    if not cp.has_section('mon'):
        cp.add_section('mon')
    if (
            not cp.has_option('mon', 'auth_allow_insecure_global_id_reclaim')
            and not cp.has_option('mon', 'auth allow insecure global id reclaim')
    ):
        cp.set('mon', 'auth_allow_insecure_global_id_reclaim', 'false')

    if ctx.single_host_defaults:
        logger.info('Adjusting default settings to suit single-host cluster...')
        # replicate across osds, not hosts
        if (
                not cp.has_option('global', 'osd_crush_chooseleaf_type')
                and not cp.has_option('global', 'osd crush chooseleaf type')
        ):
            cp.set('global', 'osd_crush_chooseleaf_type', '0')
        # replica 2x
        if (
                not cp.has_option('global', 'osd_pool_default_size')
                and not cp.has_option('global', 'osd pool default size')
        ):
            cp.set('global', 'osd_pool_default_size', '2')
        # disable mgr standby modules (so we can colocate multiple mgrs on one host)
        if not cp.has_section('mgr'):
            cp.add_section('mgr')
        if (
                not cp.has_option('mgr', 'mgr_standby_modules')
                and not cp.has_option('mgr', 'mgr standby modules')
        ):
            cp.set('mgr', 'mgr_standby_modules', 'false')
    if ctx.log_to_file:
        cp.set('global', 'log_to_file', 'true')
        cp.set('global', 'log_to_stderr', 'false')
        cp.set('global', 'log_to_journald', 'false')
        cp.set('global', 'mon_cluster_log_to_file', 'true')
        cp.set('global', 'mon_cluster_log_to_stderr', 'false')
        cp.set('global', 'mon_cluster_log_to_journald', 'false')

    cpf = StringIO()
    cp.write(cpf)
    config = cpf.getvalue()

    if ctx.registry_json or ctx.registry_url:
        command_registry_login(ctx)

    return config


def finish_bootstrap_config(
    ctx: CephadmContext,
    fsid: str,
    config: str,
    mon_id: str, mon_dir: str,
    mon_network: Optional[str], ipv6: bool,
    cli: Callable,
    cluster_network: Optional[str], ipv6_cluster_network: bool

) -> None:
    if not ctx.no_minimize_config:
        logger.info('Assimilating anything we can from ceph.conf...')
        cli([
            'config', 'assimilate-conf',
            '-i', '/var/lib/ceph/mon/ceph-%s/config' % mon_id
        ], {
            mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % mon_id
        })
        logger.info('Generating new minimal ceph.conf...')
        cli([
            'config', 'generate-minimal-conf',
            '-o', '/var/lib/ceph/mon/ceph-%s/config' % mon_id
        ], {
            mon_dir: '/var/lib/ceph/mon/ceph-%s:z' % mon_id
        })
        # re-read our minimized config
        with open(mon_dir + '/config', 'r') as f:
            config = f.read()
        logger.info('Restarting the monitor...')
        call_throws(ctx, [
            'systemctl',
            'restart',
            get_unit_name(fsid, 'mon', mon_id)
        ])
    elif 'image' in ctx and ctx.image:
        # we still want to assimilate the given container image if provided
        cli(['config', 'set', 'global', 'container_image', f'{ctx.image}'])

    if mon_network:
        cp = read_config(ctx.config)
        cfg_section = 'global' if cp.has_option('global', 'public_network') else 'mon'
        logger.info(f'Setting public_network to {mon_network} in {cfg_section} config section')
        cli(['config', 'set', cfg_section, 'public_network', mon_network])

    if cluster_network:
        logger.info(f'Setting cluster_network to {cluster_network}')
        cli(['config', 'set', 'global', 'cluster_network', cluster_network])

    if ipv6 or ipv6_cluster_network:
        logger.info('Enabling IPv6 (ms_bind_ipv6) binding')
        cli(['config', 'set', 'global', 'ms_bind_ipv6', 'true'])

    with open(ctx.output_config, 'w') as f:
        f.write(config)
    logger.info('Wrote config to %s' % ctx.output_config)
    pass


def _extract_host_info_from_applied_spec(f: Iterable[str]) -> List[Dict[str, str]]:
    # overall goal of this function is to go through an applied spec and find
    # the hostname (and addr is provided) for each host spec in the applied spec.
    # Generally, we should be able to just pass the spec to the mgr module where
    # proper yaml parsing can happen, but for host specs in particular we want to
    # be able to distribute ssh keys, which requires finding the hostname (and addr
    # if possible) for each potential host spec in the applied spec.

    specs: List[List[str]] = []
    current_spec: List[str] = []
    for line in f:
        if re.search(r'^---\s+', line):
            if current_spec:
                specs.append(current_spec)
            current_spec = []
        else:
            line = line.strip()
            if line:
                current_spec.append(line)
    if current_spec:
        specs.append(current_spec)

    host_specs: List[List[str]] = []
    for spec in specs:
        for line in spec:
            if 'service_type' in line:
                try:
                    _, type = line.split(':')
                    type = type.strip()
                    if type == 'host':
                        host_specs.append(spec)
                except ValueError as e:
                    spec_str = '\n'.join(spec)
                    logger.error(f'Failed to pull service_type from spec:\n{spec_str}. Got error: {e}')
                break
            spec_str = '\n'.join(spec)
            logger.error(f'Failed to find service_type within spec:\n{spec_str}')

    host_dicts = []
    for s in host_specs:
        host_dict = _extract_host_info_from_spec(s)
        # if host_dict is empty here, we failed to pull the hostname
        # for the host from the spec. This should have already been logged
        # so at this point we just don't want to include it in our output
        if host_dict:
            host_dicts.append(host_dict)

    return host_dicts


def _extract_host_info_from_spec(host_spec: List[str]) -> Dict[str, str]:
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
    for line in host_spec:
        for field in ['hostname', 'addr']:
            if field in line:
                try:
                    _, field_value = line.split(':')
                    field_value = field_value.strip()
                    host_dict[field] = field_value
                except ValueError as e:
                    spec_str = '\n'.join(host_spec)
                    logger.error(f'Error trying to pull {field} from host spec:\n{spec_str}. Got error: {e}')

    if 'hostname' not in host_dict:
        spec_str = '\n'.join(host_spec)
        logger.error(f'Could not find hostname in host spec:\n{spec_str}')
        return {}
    return host_dict


def _distribute_ssh_keys(ctx: CephadmContext, host_info: Dict[str, str], bootstrap_hostname: str) -> int:
    # copy ssh key to hosts in host spec (used for apply spec)
    ssh_key = CEPH_DEFAULT_PUBKEY
    if ctx.ssh_public_key:
        ssh_key = ctx.ssh_public_key.name

    if bootstrap_hostname != host_info['hostname']:
        if 'addr' in host_info:
            addr = host_info['addr']
        else:
            addr = host_info['hostname']
        out, err, code = call(ctx, ['sudo', '-u', ctx.ssh_user, 'ssh-copy-id', '-f', '-i', ssh_key, '-o StrictHostKeyChecking=no', '%s@%s' % (ctx.ssh_user, addr)])
        if code:
            logger.error('\nCopying ssh key to host %s at address %s failed!\n' % (host_info['hostname'], addr))
            return 1
        else:
            logger.info('Added ssh key to host %s at address %s' % (host_info['hostname'], addr))
    return 0


def save_cluster_config(ctx: CephadmContext, uid: int, gid: int, fsid: str) -> None:
    """Save cluster configuration to the per fsid directory """
    def copy_file(src: str, dst: str) -> None:
        if src:
            shutil.copyfile(src, dst)

    conf_dir = f'{ctx.data_dir}/{fsid}/{CEPH_CONF_DIR}'
    makedirs(conf_dir, uid, gid, DATA_DIR_MODE)
    if os.path.exists(conf_dir):
        logger.info(f'Saving cluster configuration to {conf_dir} directory')
        copy_file(ctx.output_config, os.path.join(conf_dir, CEPH_CONF))
        copy_file(ctx.output_keyring, os.path.join(conf_dir, CEPH_KEYRING))
        # ctx.output_pub_ssh_key may not exist if user has provided custom ssh keys
        if (os.path.exists(ctx.output_pub_ssh_key)):
            copy_file(ctx.output_pub_ssh_key, os.path.join(conf_dir, CEPH_PUBKEY))
    else:
        logger.warning(f'Cannot create cluster configuration directory {conf_dir}')


def rollback(func: FuncT) -> FuncT:
    """
    """
    @wraps(func)
    def _rollback(ctx: CephadmContext) -> Any:
        try:
            return func(ctx)
        except ClusterAlreadyExists:
            # another cluster with the provided fsid already exists: don't remove.
            raise
        except (KeyboardInterrupt, Exception) as e:
            logger.error(f'{type(e).__name__}: {e}')
            if ctx.cleanup_on_failure:
                logger.info('\n\n'
                            '\t***************\n'
                            '\tCephadm hit an issue during cluster installation. Current cluster files will be deleted automatically,\n'
                            '\tto disable this behaviour do not pass the --cleanup-on-failure flag. In case of any previous\n'
                            '\tbroken installation user must use the following command to completely delete the broken cluster:\n\n'
                            '\t> cephadm rm-cluster --force --zap-osds --fsid <fsid>\n\n'
                            '\tfor more information please refer to https://docs.ceph.com/en/latest/cephadm/operations/#purging-a-cluster\n'
                            '\t***************\n\n')
                _rm_cluster(ctx, keep_logs=False, zap_osds=False)
            else:
                logger.info('\n\n'
                            '\t***************\n'
                            '\tCephadm hit an issue during cluster installation. Current cluster files will NOT BE DELETED automatically to change\n'
                            '\tthis behaviour you can pass the --cleanup-on-failure. To remove this broken cluster manually please run:\n\n'
                            f'\t   > cephadm rm-cluster --force --fsid {ctx.fsid}\n\n'
                            '\tin case of any previous broken installation user must use the rm-cluster command to delete the broken cluster:\n\n'
                            '\t   > cephadm rm-cluster --force --zap-osds --fsid <fsid>\n\n'
                            '\tfor more information please refer to https://docs.ceph.com/en/latest/cephadm/operations/#purging-a-cluster\n'
                            '\t***************\n\n')
            raise
    return cast(FuncT, _rollback)


@rollback
@default_image
def command_bootstrap(ctx):
    # type: (CephadmContext) -> int

    ctx.error_code = 0

    if not ctx.output_config:
        ctx.output_config = os.path.join(ctx.output_dir, CEPH_CONF)
    if not ctx.output_keyring:
        ctx.output_keyring = os.path.join(ctx.output_dir, CEPH_KEYRING)
    if not ctx.output_pub_ssh_key:
        ctx.output_pub_ssh_key = os.path.join(ctx.output_dir, CEPH_PUBKEY)

    if (
        (bool(ctx.ssh_private_key) is not bool(ctx.ssh_public_key))
        and (bool(ctx.ssh_private_key) is not bool(ctx.ssh_signed_cert))
    ):
        raise Error('--ssh-private-key must be passed with either --ssh-public-key in the case of standard pubkey '
                    'authentication or with --ssh-signed-cert in the case of CA signed signed keys or not provided at all.')

    if (bool(ctx.ssh_public_key) and bool(ctx.ssh_signed_cert)):
        raise Error('--ssh-public-key and --ssh-signed-cert are mututally exclusive. --ssh-public-key is intended '
                    'for standard pubkey encryption where the public key is set as an authorized key on cluster hosts. '
                    '--ssh-signed-cert is intended for the CA signed keys use case where cluster hosts are configured to trust '
                    'a CA pub key and authentication during SSH is done by authenticating the signed cert, requiring no '
                    'public key to be installed on the cluster hosts.')

    if ctx.fsid:
        data_dir_base = os.path.join(ctx.data_dir, ctx.fsid)
        if os.path.exists(data_dir_base):
            raise ClusterAlreadyExists(f"A cluster with the same fsid '{ctx.fsid}' already exists.")
        else:
            logger.warning('Specifying an fsid for your cluster offers no advantages and may increase the likelihood of fsid conflicts.')

    # initial vars
    ctx.fsid = ctx.fsid or make_fsid()
    fsid = ctx.fsid
    if not is_fsid(fsid):
        raise Error('not an fsid: %s' % fsid)

    # verify output files
    for f in [ctx.output_config, ctx.output_keyring, ctx.output_pub_ssh_key]:
        if not ctx.allow_overwrite:
            if os.path.exists(f):
                raise ClusterAlreadyExists('%s already exists; delete or pass --allow-overwrite to overwrite' % f)
        dirname = os.path.dirname(f)
        if dirname and not os.path.exists(dirname):
            fname = os.path.basename(f)
            logger.info(f'Creating directory {dirname} for {fname}')
            try:
                # use makedirs to create intermediate missing dirs
                os.makedirs(dirname, 0o755)
            except PermissionError:
                raise Error(f'Unable to create {dirname} due to permissions failure. Retry with root, or sudo or preallocate the directory.')

    (user_conf, _) = get_config_and_keyring(ctx)

    if ctx.ssh_user != 'root':
        check_ssh_connectivity(ctx)

    if not ctx.skip_prepare_host:
        command_prepare_host(ctx)
    else:
        logger.info('Skip prepare_host')

    logger.info('Cluster fsid: %s' % fsid)
    hostname = get_hostname()
    if '.' in hostname and not ctx.allow_fqdn_hostname:
        raise Error('hostname is a fully qualified domain name (%s); either fix (e.g., "sudo hostname %s" or similar) or pass --allow-fqdn-hostname' % (hostname, hostname.split('.')[0]))
    mon_id = ctx.mon_id or get_short_hostname()
    mgr_id = ctx.mgr_id or generate_service_id()

    lock = FileLock(ctx, fsid)
    lock.acquire()

    (addr_arg, ipv6, mon_network) = prepare_mon_addresses(ctx)
    cluster_network, ipv6_cluster_network = prepare_cluster_network(ctx)

    config = prepare_bootstrap_config(ctx, fsid, addr_arg, ctx.image)

    if not ctx.skip_pull:
        try:
            _pull_image(ctx, ctx.image)
        except UnauthorizedRegistryError:
            err_str = 'Failed to pull container image. Check that correct registry credentials are provided in bootstrap by --registry-url, --registry-username, --registry-password, or supply --registry-json with credentials'
            logger.debug(f'Pulling image for bootstrap on {hostname} failed: {err_str}')
            raise Error(err_str)

    image_ver = CephContainer(ctx, ctx.image, 'ceph', ['--version']).run().strip()
    logger.info(f'Ceph version: {image_ver}')

    if not ctx.allow_mismatched_release:
        image_release = image_ver.split()[4]
        if image_release not in \
                [DEFAULT_IMAGE_RELEASE, LATEST_STABLE_RELEASE]:
            raise Error(
                f'Container release {image_release} != cephadm release {DEFAULT_IMAGE_RELEASE};'
                ' please use matching version of cephadm (pass --allow-mismatched-release to continue anyway)'
            )

    logger.info('Extracting ceph user uid/gid from container image...')
    (uid, gid) = extract_uid_gid(ctx)

    # create some initial keys
    (mon_key, mgr_key, admin_key, bootstrap_keyring, admin_keyring) = create_initial_keys(ctx, uid, gid, mgr_id)

    monmap = create_initial_monmap(ctx, uid, gid, fsid, mon_id, addr_arg)
    (mon_dir, log_dir) = prepare_create_mon(ctx, uid, gid, fsid, mon_id,
                                            bootstrap_keyring.name, monmap.name)

    with write_new(mon_dir + '/config', owner=(uid, gid)) as f:
        f.write(config)

    make_var_run(ctx, fsid, uid, gid)
    create_mon(ctx, uid, gid, fsid, mon_id)

    # config to issue various CLI commands
    tmp_config = write_tmp(config, uid, gid)

    # a CLI helper to reduce our typing
    def cli(cmd, extra_mounts={}, timeout=DEFAULT_TIMEOUT, verbosity=CallVerbosity.VERBOSE_ON_FAILURE):
        # type: (List[str], Dict[str, str], Optional[int], CallVerbosity) -> str
        mounts = {
            log_dir: '/var/log/ceph:z',
            admin_keyring.name: '/etc/ceph/ceph.client.admin.keyring:z',
            tmp_config.name: '/etc/ceph/ceph.conf:z',
        }
        for k, v in extra_mounts.items():
            mounts[k] = v
        timeout = timeout or ctx.timeout
        return CephContainer(
            ctx,
            image=ctx.image,
            entrypoint='/usr/bin/ceph',
            args=cmd,
            volume_mounts=mounts,
        ).run(timeout=timeout, verbosity=verbosity)

    wait_for_mon(ctx, mon_id, mon_dir, admin_keyring.name, tmp_config.name)

    finish_bootstrap_config(ctx, fsid, config, mon_id, mon_dir,
                            mon_network, ipv6, cli,
                            cluster_network, ipv6_cluster_network)

    # output files
    with write_new(ctx.output_keyring) as f:
        f.write('[client.admin]\n'
                '\tkey = ' + admin_key + '\n')
    logger.info('Wrote keyring to %s' % ctx.output_keyring)

    # create mgr
    create_mgr(ctx, uid, gid, fsid, mgr_id, mgr_key, config, cli)

    if user_conf:
        # user given config settings were already assimilated earlier
        # but if the given settings contained any attributes in
        # the mgr (e.g. mgr/cephadm/container_image_prometheus)
        # they don't seem to be stored if there isn't a mgr yet.
        # Since re-assimilating the same conf settings should be
        # idempotent we can just do it again here.
        with tempfile.NamedTemporaryFile(buffering=0) as tmp:
            tmp.write(user_conf.encode('utf-8'))
            cli(['config', 'assimilate-conf',
                 '-i', '/var/lib/ceph/user.conf'],
                {tmp.name: '/var/lib/ceph/user.conf:z'})

    # wait for mgr to restart (after enabling a module)
    def wait_for_mgr_restart() -> None:
        # first get latest mgrmap epoch from the mon.  try newer 'mgr
        # stat' command first, then fall back to 'mgr dump' if
        # necessary
        try:
            j = json_loads_retry(lambda: cli(['mgr', 'stat'], verbosity=CallVerbosity.QUIET_UNLESS_ERROR))
        except Exception:
            j = json_loads_retry(lambda: cli(['mgr', 'dump'], verbosity=CallVerbosity.QUIET_UNLESS_ERROR))
        epoch = j['epoch']

        # wait for mgr to have it
        logger.info('Waiting for the mgr to restart...')

        def mgr_has_latest_epoch():
            # type: () -> bool
            try:
                out = cli(['tell', 'mgr', 'mgr_status'])
                j = json.loads(out)
                return j['mgrmap_epoch'] >= epoch
            except Exception as e:
                logger.debug('tell mgr mgr_status failed: %s' % e)
                return False
        is_available(ctx, 'mgr epoch %d' % epoch, mgr_has_latest_epoch)

    enable_cephadm_mgr_module(cli, wait_for_mgr_restart)

    # ssh
    if not ctx.skip_ssh:
        prepare_ssh(ctx, cli, wait_for_mgr_restart)

    if ctx.registry_url and ctx.registry_username and ctx.registry_password:
        registry_credentials = {'url': ctx.registry_url, 'username': ctx.registry_username, 'password': ctx.registry_password}
        cli(['config-key', 'set', 'mgr/cephadm/registry_credentials', json.dumps(registry_credentials)])

    cli(['config', 'set', 'mgr', 'mgr/cephadm/container_init', str(ctx.container_init), '--force'])

    if not ctx.skip_dashboard:
        prepare_dashboard(ctx, uid, gid, cli, wait_for_mgr_restart)

    if ctx.output_config == CEPH_DEFAULT_CONF and not ctx.skip_admin_label and not ctx.no_minimize_config:
        logger.info('Enabling client.admin keyring and conf on hosts with "admin" label')
        try:
            cli(['orch', 'client-keyring', 'set', 'client.admin', 'label:_admin'])
            cli(['orch', 'host', 'label', 'add', get_hostname(), '_admin'])
        except Exception:
            logger.info('Unable to set up "admin" label; assuming older version of Ceph')

    if ctx.apply_spec:
        logger.info('Applying %s to cluster' % ctx.apply_spec)
        # copy ssh key to hosts in spec file
        with open(ctx.apply_spec) as f:
            host_dicts = _extract_host_info_from_applied_spec(f)
            for h in host_dicts:
                if ctx.ssh_signed_cert:
                    logger.info('Key distribution is not supported for signed CA key setups. Skipping ...')
                else:
                    _distribute_ssh_keys(ctx, h, hostname)

        mounts = {}
        mounts[pathify(ctx.apply_spec)] = '/tmp/spec.yml:ro'
        try:
            out = cli(['orch', 'apply', '-i', '/tmp/spec.yml'], extra_mounts=mounts)
            logger.info(out)
        except Exception:
            ctx.error_code = -errno.EINVAL
            logger.info('\nApplying %s to cluster failed!\n' % ctx.apply_spec)

    save_cluster_config(ctx, uid, gid, fsid)

    # enable autotune for osd_memory_target
    logger.info('Enabling autotune for osd_memory_target')
    cli(['config', 'set', 'osd', 'osd_memory_target_autotune', 'true'])

    # Notify the Dashboard to show the 'Expand cluster' page on first log in.
    cli(['config-key', 'set', 'mgr/dashboard/cluster/status', 'INSTALLED'])

    logger.info('You can access the Ceph CLI as following in case of multi-cluster or non-default config:\n\n'
                '\tsudo %s shell --fsid %s -c %s -k %s\n' % (
                    sys.argv[0],
                    fsid,
                    ctx.output_config,
                    ctx.output_keyring))

    logger.info('Or, if you are only running a single cluster on this host:\n\n\tsudo %s shell \n' % (sys.argv[0]))

    logger.info('Please consider enabling telemetry to help improve Ceph:\n\n'
                '\tceph telemetry on\n\n'
                'For more information see:\n\n'
                '\thttps://docs.ceph.com/en/latest/mgr/telemetry/\n')
    logger.info('Bootstrap complete.')
    return ctx.error_code

##################################


def command_registry_login(ctx: CephadmContext) -> int:
    if ctx.registry_json:
        logger.info('Pulling custom registry login info from %s.' % ctx.registry_json)
        d = get_parm(ctx.registry_json)
        if d.get('url') and d.get('username') and d.get('password'):
            ctx.registry_url = d.get('url')
            ctx.registry_username = d.get('username')
            ctx.registry_password = d.get('password')
            registry_login(ctx, ctx.registry_url, ctx.registry_username, ctx.registry_password)
        else:
            raise Error('json provided for custom registry login did not include all necessary fields. '
                        'Please setup json file as\n'
                        '{\n'
                        ' "url": "REGISTRY_URL",\n'
                        ' "username": "REGISTRY_USERNAME",\n'
                        ' "password": "REGISTRY_PASSWORD"\n'
                        '}\n')
    elif ctx.registry_url and ctx.registry_username and ctx.registry_password:
        registry_login(ctx, ctx.registry_url, ctx.registry_username, ctx.registry_password)
    else:
        raise Error('Invalid custom registry arguments received. To login to a custom registry include '
                    '--registry-url, --registry-username and --registry-password '
                    'options or --registry-json option')
    return 0


def registry_login(ctx: CephadmContext, url: Optional[str], username: Optional[str], password: Optional[str]) -> None:
    logger.info('Logging into custom registry.')
    try:
        engine = ctx.container_engine
        cmd = [engine.path, 'login',
               '-u', username, '-p', password,
               url]
        if isinstance(engine, Podman):
            cmd.append('--authfile=/etc/ceph/podman-auth.json')
        out, _, _ = call_throws(ctx, cmd)
        if isinstance(engine, Podman):
            os.chmod('/etc/ceph/podman-auth.json', DEFAULT_MODE)
    except Exception:
        raise Error('Failed to login to custom registry @ %s as %s with given password' % (ctx.registry_url, ctx.registry_username))

##################################


def extract_uid_gid_monitoring(ctx, daemon_type):
    # type: (CephadmContext, str) -> Tuple[int, int]

    if daemon_type == 'prometheus':
        uid, gid = extract_uid_gid(ctx, file_path='/etc/prometheus')
    elif daemon_type == 'node-exporter':
        uid, gid = 65534, 65534
    elif daemon_type == 'grafana':
        uid, gid = extract_uid_gid(ctx, file_path='/var/lib/grafana')
    elif daemon_type == 'loki':
        uid, gid = extract_uid_gid(ctx, file_path='/etc/loki')
    elif daemon_type == 'promtail':
        uid, gid = extract_uid_gid(ctx, file_path='/etc/promtail')
    elif daemon_type == 'alertmanager':
        uid, gid = extract_uid_gid(ctx, file_path=['/etc/alertmanager', '/etc/prometheus'])
    else:
        raise Error('{} not implemented yet'.format(daemon_type))
    return uid, gid


def get_deployment_container(ctx: CephadmContext,
                             fsid: str, daemon_type: str, daemon_id: Union[int, str],
                             privileged: bool = False,
                             ptrace: bool = False,
                             container_args: Optional[List[str]] = None) -> 'CephContainer':
    # wrapper for get_container specifically for containers made during the `cephadm deploy`
    # command. Adds some extra things such as extra container args and custom config files
    c = get_container(ctx, fsid, daemon_type, daemon_id, privileged, ptrace, container_args)
    if 'extra_container_args' in ctx and ctx.extra_container_args:
        c.container_args.extend(ctx.extra_container_args)
    if 'extra_entrypoint_args' in ctx and ctx.extra_entrypoint_args:
        c.args.extend(ctx.extra_entrypoint_args)
    ccfiles = fetch_custom_config_files(ctx)
    if ccfiles:
        mandatory_keys = ['mount_path', 'content']
        for conf in ccfiles:
            if all(k in conf for k in mandatory_keys):
                mount_path = conf['mount_path']
                file_path = os.path.join(
                    ctx.data_dir,
                    fsid,
                    'custom_config_files',
                    f'{daemon_type}.{daemon_id}',
                    os.path.basename(mount_path)
                )
                c.volume_mounts[file_path] = mount_path
    return c


def get_deployment_type(ctx: CephadmContext, daemon_type: str, daemon_id: str) -> DeploymentType:
    deployment_type: DeploymentType = DeploymentType.DEFAULT
    if ctx.reconfig:
        deployment_type = DeploymentType.RECONFIG
    unit_name = get_unit_name(ctx.fsid, daemon_type, daemon_id)
    (_, state, _) = check_unit(ctx, unit_name)
    if state == 'running' or is_container_running(ctx, CephContainer.for_daemon(ctx, ctx.fsid, daemon_type, daemon_id, 'bash')):
        # if reconfig was set, that takes priority over redeploy. If
        # this is considered a fresh deployment at this stage,
        # mark it as a redeploy to avoid port checking
        if deployment_type == DeploymentType.DEFAULT:
            deployment_type = DeploymentType.REDEPLOY

    logger.info(f'{deployment_type.value} daemon {ctx.name} ...')

    return deployment_type


@default_image
@deprecated_command
def command_deploy(ctx):
    # type: (CephadmContext) -> None
    _common_deploy(ctx)


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
    logger.debug('Loaded deploy configuration: %r', config_data)
    return config_data


def apply_deploy_config_to_ctx(
    config_data: Dict[str, Any],
    ctx: CephadmContext,
) -> None:
    """Bind properties taken from the config_data dictionary to our ctx,
    similar to how cli options on `deploy` are bound to the context.
    """
    ctx.name = config_data['name']
    image = config_data.get('image', '')
    if image:
        ctx.image = image
    if 'fsid' in config_data:
        ctx.fsid = config_data['fsid']
    if 'meta' in config_data:
        ctx.meta_properties = config_data['meta']
    if 'config_blobs' in config_data:
        ctx.config_blobs = config_data['config_blobs']

    # many functions don't check that an attribute is set on the ctx
    # (with getattr or the '__contains__' func on ctx).
    # This reuses the defaults from the CLI options so we don't
    # have to repeat things and they can stay in sync.
    facade = ArgumentFacade()
    _add_deploy_parser_args(facade)
    facade.apply(ctx)
    for key, value in config_data.get('params', {}).items():
        if key not in facade.defaults:
            logger.warning('unexpected parameter: %r=%r', key, value)
        setattr(ctx, key, value)
    update_default_image(ctx)
    logger.debug('Determined image: %r', ctx.image)


def command_deploy_from(ctx: CephadmContext) -> None:
    """The deploy-from command is similar to deploy but sources nearly all
    configuration parameters from an input JSON configuration file.
    """
    config_data = read_configuration_source(ctx)
    apply_deploy_config_to_ctx(config_data, ctx)
    _common_deploy(ctx)


def _common_deploy(ctx: CephadmContext) -> None:
    daemon_type, daemon_id = ctx.name.split('.', 1)
    if daemon_type not in get_supported_daemons():
        raise Error('daemon type %s not recognized' % daemon_type)

    lock = FileLock(ctx, ctx.fsid)
    lock.acquire()

    deployment_type = get_deployment_type(ctx, daemon_type, daemon_id)

    # Migrate sysctl conf files from /usr/lib to /etc
    migrate_sysctl_dir(ctx, ctx.fsid)

    # Get and check ports explicitly required to be opened
    endpoints = fetch_tcp_ports(ctx)
    _dispatch_deploy(ctx, daemon_type, daemon_id, endpoints, deployment_type)


def _dispatch_deploy(
    ctx: CephadmContext,
    daemon_type: str,
    daemon_id: str,
    daemon_endpoints: List[EndPoint],
    deployment_type: DeploymentType,
) -> None:
    if daemon_type in Ceph.daemons:
        config, keyring = get_config_and_keyring(ctx)
        uid, gid = extract_uid_gid(ctx)
        make_var_run(ctx, ctx.fsid, uid, gid)

        config_json = fetch_configs(ctx)

        c = get_deployment_container(ctx, ctx.fsid, daemon_type, daemon_id,
                                     ptrace=ctx.allow_ptrace)

        if daemon_type == 'mon' and config_json is not None:
            if 'crush_location' in config_json:
                c_loc = config_json['crush_location']
                # was originally "c.args.extend(['--set-crush-location', c_loc])"
                # but that doesn't seem to persist in the object after it's passed
                # in further function calls
                c.args = c.args + ['--set-crush-location', c_loc]

        deploy_daemon(ctx, ctx.fsid, daemon_type, daemon_id, c, uid, gid,
                      config=config, keyring=keyring,
                      osd_fsid=ctx.osd_fsid,
                      deployment_type=deployment_type,
                      endpoints=daemon_endpoints)

    elif daemon_type in Monitoring.components:
        # monitoring daemon - prometheus, grafana, alertmanager, node-exporter
        # Default Checks
        # make sure provided config-json is sufficient
        config = fetch_configs(ctx)  # type: ignore
        required_files = Monitoring.components[daemon_type].get('config-json-files', list())
        required_args = Monitoring.components[daemon_type].get('config-json-args', list())
        if required_files:
            if not config or not all(c in config.get('files', {}).keys() for c in required_files):  # type: ignore
                raise Error('{} deployment requires config-json which must '
                            'contain file content for {}'.format(daemon_type.capitalize(), ', '.join(required_files)))
        if required_args:
            if not config or not all(c in config.keys() for c in required_args):  # type: ignore
                raise Error('{} deployment requires config-json which must '
                            'contain arg for {}'.format(daemon_type.capitalize(), ', '.join(required_args)))

        uid, gid = extract_uid_gid_monitoring(ctx, daemon_type)
        c = get_deployment_container(ctx, ctx.fsid, daemon_type, daemon_id)
        deploy_daemon(ctx, ctx.fsid, daemon_type, daemon_id, c, uid, gid,
                      deployment_type=deployment_type,
                      endpoints=daemon_endpoints)

    elif daemon_type == NFSGanesha.daemon_type:
        # only check ports if this is a fresh deployment
        if deployment_type == DeploymentType.DEFAULT and not daemon_endpoints:
            nfs_ports = list(NFSGanesha.port_map.values())
            daemon_endpoints = [EndPoint('0.0.0.0', p) for p in nfs_ports]

        config, keyring = get_config_and_keyring(ctx)
        # TODO: extract ganesha uid/gid (997, 994) ?
        uid, gid = extract_uid_gid(ctx)
        c = get_deployment_container(ctx, ctx.fsid, daemon_type, daemon_id)
        deploy_daemon(ctx, ctx.fsid, daemon_type, daemon_id, c, uid, gid,
                      config=config, keyring=keyring,
                      deployment_type=deployment_type,
                      endpoints=daemon_endpoints)

    elif daemon_type == CephIscsi.daemon_type:
        config, keyring = get_config_and_keyring(ctx)
        uid, gid = extract_uid_gid(ctx)
        c = get_deployment_container(ctx, ctx.fsid, daemon_type, daemon_id)
        deploy_daemon(ctx, ctx.fsid, daemon_type, daemon_id, c, uid, gid,
                      config=config, keyring=keyring,
                      deployment_type=deployment_type,
                      endpoints=daemon_endpoints)
    elif daemon_type == CephNvmeof.daemon_type:
        config, keyring = get_config_and_keyring(ctx)
        uid, gid = 167, 167  # TODO: need to get properly the uid/gid
        c = get_deployment_container(ctx, ctx.fsid, daemon_type, daemon_id)
        deploy_daemon(ctx, ctx.fsid, daemon_type, daemon_id, c, uid, gid,
                      config=config, keyring=keyring,
                      deployment_type=deployment_type,
                      endpoints=daemon_endpoints)
    elif daemon_type in Tracing.components:
        uid, gid = 65534, 65534
        c = get_container(ctx, ctx.fsid, daemon_type, daemon_id)
        deploy_daemon(ctx, ctx.fsid, daemon_type, daemon_id, c, uid, gid,
                      deployment_type=deployment_type,
                      endpoints=daemon_endpoints)
    elif daemon_type == HAproxy.daemon_type:
        haproxy = HAproxy.init(ctx, ctx.fsid, daemon_id)
        uid, gid = haproxy.extract_uid_gid_haproxy()
        c = get_deployment_container(ctx, ctx.fsid, daemon_type, daemon_id)
        deploy_daemon(ctx, ctx.fsid, daemon_type, daemon_id, c, uid, gid,
                      deployment_type=deployment_type,
                      endpoints=daemon_endpoints)

    elif daemon_type == Keepalived.daemon_type:
        keepalived = Keepalived.init(ctx, ctx.fsid, daemon_id)
        uid, gid = keepalived.extract_uid_gid_keepalived()
        c = get_deployment_container(ctx, ctx.fsid, daemon_type, daemon_id)
        deploy_daemon(ctx, ctx.fsid, daemon_type, daemon_id, c, uid, gid,
                      deployment_type=deployment_type,
                      endpoints=daemon_endpoints)

    elif daemon_type == CustomContainer.daemon_type:
        cc = CustomContainer.init(ctx, ctx.fsid, daemon_id)
        # only check ports if this is a fresh deployment
        if deployment_type == DeploymentType.DEFAULT:
            daemon_endpoints.extend([EndPoint('0.0.0.0', p) for p in cc.ports])
        c = get_deployment_container(ctx, ctx.fsid, daemon_type, daemon_id,
                                     privileged=cc.privileged,
                                     ptrace=ctx.allow_ptrace)
        deploy_daemon(ctx, ctx.fsid, daemon_type, daemon_id, c,
                      uid=cc.uid, gid=cc.gid, config=None,
                      keyring=None,
                      deployment_type=deployment_type,
                      endpoints=daemon_endpoints)

    elif daemon_type == CephadmAgent.daemon_type:
        # get current user gid and uid
        uid = os.getuid()
        gid = os.getgid()
        deploy_daemon(ctx, ctx.fsid, daemon_type, daemon_id, None,
                      uid, gid,
                      deployment_type=deployment_type,
                      endpoints=daemon_endpoints)

    elif daemon_type == SNMPGateway.daemon_type:
        sc = SNMPGateway.init(ctx, ctx.fsid, daemon_id)
        c = get_deployment_container(ctx, ctx.fsid, daemon_type, daemon_id)
        deploy_daemon(ctx, ctx.fsid, daemon_type, daemon_id, c,
                      sc.uid, sc.gid,
                      deployment_type=deployment_type,
                      endpoints=daemon_endpoints)

    else:
        raise Error('daemon type {} not implemented in command_deploy function'
                    .format(daemon_type))

##################################


@infer_image
def command_run(ctx):
    # type: (CephadmContext) -> int
    (daemon_type, daemon_id) = ctx.name.split('.', 1)
    c = get_container(ctx, ctx.fsid, daemon_type, daemon_id)
    command = c.run_cmd()
    return call_timeout(ctx, command, ctx.timeout)

##################################


@infer_fsid
@infer_config
@infer_image
@validate_fsid
def command_shell(ctx):
    # type: (CephadmContext) -> int
    cp = read_config(ctx.config)
    if cp.has_option('global', 'fsid') and \
       cp.get('global', 'fsid') != ctx.fsid:
        raise Error('fsid does not match ceph.conf')

    if ctx.name:
        if '.' in ctx.name:
            (daemon_type, daemon_id) = ctx.name.split('.', 1)
        else:
            daemon_type = ctx.name
            daemon_id = None
    else:
        daemon_type = 'osd'  # get the most mounts
        daemon_id = None

    if ctx.fsid and daemon_type in Ceph.daemons:
        make_log_dir(ctx, ctx.fsid)

    if daemon_id and not ctx.fsid:
        raise Error('must pass --fsid to specify cluster')

    # in case a dedicated keyring for the specified fsid is found we us it.
    # Otherwise, use /etc/ceph files by default, if present.  We do this instead of
    # making these defaults in the arg parser because we don't want an error
    # if they don't exist.
    if not ctx.keyring:
        keyring_file = f'{ctx.data_dir}/{ctx.fsid}/{CEPH_CONF_DIR}/{CEPH_KEYRING}'
        if os.path.exists(keyring_file):
            ctx.keyring = keyring_file
        elif os.path.exists(CEPH_DEFAULT_KEYRING):
            ctx.keyring = CEPH_DEFAULT_KEYRING

    container_args: List[str] = ['-i']
    mounts = get_container_mounts(ctx, ctx.fsid, daemon_type, daemon_id,
                                  no_config=True if ctx.config else False)
    binds = get_container_binds(ctx, ctx.fsid, daemon_type, daemon_id)
    if ctx.config:
        mounts[pathify(ctx.config)] = '/etc/ceph/ceph.conf:z'
    if ctx.keyring:
        mounts[pathify(ctx.keyring)] = '/etc/ceph/ceph.keyring:z'
    if ctx.mount:
        for _mount in ctx.mount:
            split_src_dst = _mount.split(':')
            mount = pathify(split_src_dst[0])
            filename = os.path.basename(split_src_dst[0])
            if len(split_src_dst) > 1:
                dst = split_src_dst[1]
                if len(split_src_dst) == 3:
                    dst = '{}:{}'.format(dst, split_src_dst[2])
                mounts[mount] = dst
            else:
                mounts[mount] = '/mnt/{}'.format(filename)
    if ctx.command:
        command = ctx.command
    else:
        command = ['bash']
        container_args += [
            '-t',
            '-e', 'LANG=C',
            '-e', 'PS1=%s' % CUSTOM_PS1,
        ]
        if ctx.fsid:
            home = os.path.join(ctx.data_dir, ctx.fsid, 'home')
            if not os.path.exists(home):
                logger.debug('Creating root home at %s' % home)
                makedirs(home, 0, 0, 0o660)
                if os.path.exists('/etc/skel'):
                    for f in os.listdir('/etc/skel'):
                        if f.startswith('.bash'):
                            shutil.copyfile(os.path.join('/etc/skel', f),
                                            os.path.join(home, f))
            mounts[home] = '/root'

    for i in ctx.volume:
        a, b = i.split(':', 1)
        mounts[a] = b

    c = CephContainer(
        ctx,
        image=ctx.image,
        entrypoint='doesnotmatter',
        args=[],
        container_args=container_args,
        volume_mounts=mounts,
        bind_mounts=binds,
        envs=ctx.env,
        privileged=True)
    command = c.shell_cmd(command)

    if ctx.dry_run:
        print(' '.join(shlex.quote(arg) for arg in command))
        return 0

    return call_timeout(ctx, command, ctx.timeout)

##################################


@infer_fsid
def command_enter(ctx):
    # type: (CephadmContext) -> int
    if not ctx.fsid:
        raise Error('must pass --fsid to specify cluster')
    (daemon_type, daemon_id) = ctx.name.split('.', 1)
    container_args = ['-i']  # type: List[str]
    if ctx.command:
        command = ctx.command
    else:
        command = ['sh']
        container_args += [
            '-t',
            '-e', 'LANG=C',
            '-e', 'PS1=%s' % CUSTOM_PS1,
        ]
    c = CephContainer(
        ctx,
        image=ctx.image,
        entrypoint='doesnotmatter',
        container_args=container_args,
        cname='ceph-%s-%s.%s' % (ctx.fsid, daemon_type, daemon_id),
    )
    command = c.exec_cmd(command)
    return call_timeout(ctx, command, ctx.timeout)

##################################


@infer_fsid
@infer_image
@validate_fsid
def command_ceph_volume(ctx):
    # type: (CephadmContext) -> None
    cp = read_config(ctx.config)
    if cp.has_option('global', 'fsid') and \
       cp.get('global', 'fsid') != ctx.fsid:
        raise Error('fsid does not match ceph.conf')

    if ctx.fsid:
        make_log_dir(ctx, ctx.fsid)

        lock = FileLock(ctx, ctx.fsid)
        lock.acquire()

    (uid, gid) = (0, 0)  # ceph-volume runs as root
    mounts = get_container_mounts(ctx, ctx.fsid, 'osd', None)

    tmp_config = None
    tmp_keyring = None

    (config, keyring) = get_config_and_keyring(ctx)

    if config:
        # tmp config file
        tmp_config = write_tmp(config, uid, gid)
        mounts[tmp_config.name] = '/etc/ceph/ceph.conf:z'

    if keyring:
        # tmp keyring file
        tmp_keyring = write_tmp(keyring, uid, gid)
        mounts[tmp_keyring.name] = '/var/lib/ceph/bootstrap-osd/ceph.keyring:z'

    c = get_ceph_volume_container(
        ctx,
        envs=ctx.env,
        args=ctx.command,
        volume_mounts=mounts,
    )

    out, err, code = call_throws(ctx, c.run_cmd(), verbosity=CallVerbosity.QUIET_UNLESS_ERROR)
    if not code:
        print(out)

##################################


@infer_fsid
def command_unit(ctx):
    # type: (CephadmContext) -> int
    if not ctx.fsid:
        raise Error('must pass --fsid to specify cluster')

    unit_name = get_unit_name_by_daemon_name(ctx, ctx.fsid, ctx.name)

    _, _, code = call(
        ctx,
        ['systemctl', ctx.command, unit_name],
        verbosity=CallVerbosity.VERBOSE,
        desc=''
    )
    return code

##################################


@infer_fsid
def command_logs(ctx):
    # type: (CephadmContext) -> None
    if not ctx.fsid:
        raise Error('must pass --fsid to specify cluster')

    unit_name = get_unit_name_by_daemon_name(ctx, ctx.fsid, ctx.name)

    cmd = [find_program('journalctl')]
    cmd.extend(['-u', unit_name])
    if ctx.command:
        cmd.extend(ctx.command)

    # call this directly, without our wrapper, so that we get an unmolested
    # stdout with logger prefixing.
    logger.debug('Running command: %s' % ' '.join(cmd))
    subprocess.call(cmd, env=os.environ.copy())  # type: ignore

##################################


def list_networks(ctx):
    # type: (CephadmContext) -> Dict[str,Dict[str, Set[str]]]

    # sadly, 18.04's iproute2 4.15.0-2ubun doesn't support the -j flag,
    # so we'll need to use a regex to parse 'ip' command output.
    #
    # out, _, _ = call_throws(['ip', '-j', 'route', 'ls'])
    # j = json.loads(out)
    # for x in j:
    res = _list_ipv4_networks(ctx)
    res.update(_list_ipv6_networks(ctx))
    return res


def _list_ipv4_networks(ctx: CephadmContext) -> Dict[str, Dict[str, Set[str]]]:
    execstr: Optional[str] = find_executable('ip')
    if not execstr:
        raise FileNotFoundError("unable to find 'ip' command")
    out, _, _ = call_throws(ctx, [execstr, 'route', 'ls'], verbosity=CallVerbosity.QUIET_UNLESS_ERROR)
    return _parse_ipv4_route(out)


def _parse_ipv4_route(out: str) -> Dict[str, Dict[str, Set[str]]]:
    r = {}  # type: Dict[str, Dict[str, Set[str]]]
    p = re.compile(r'^(\S+) (?:via \S+)? ?dev (\S+) (.*)scope link (.*)src (\S+)')
    for line in out.splitlines():
        m = p.findall(line)
        if not m:
            continue
        net = m[0][0]
        if '/' not in net:  # aggregate /32 mask for single host sub-networks
            net += '/32'
        iface = m[0][1]
        ip = m[0][4]
        if net not in r:
            r[net] = {}
        if iface not in r[net]:
            r[net][iface] = set()
        r[net][iface].add(ip)
    return r


def _list_ipv6_networks(ctx: CephadmContext) -> Dict[str, Dict[str, Set[str]]]:
    execstr: Optional[str] = find_executable('ip')
    if not execstr:
        raise FileNotFoundError("unable to find 'ip' command")
    routes, _, _ = call_throws(ctx, [execstr, '-6', 'route', 'ls'], verbosity=CallVerbosity.QUIET_UNLESS_ERROR)
    ips, _, _ = call_throws(ctx, [execstr, '-6', 'addr', 'ls'], verbosity=CallVerbosity.QUIET_UNLESS_ERROR)
    return _parse_ipv6_route(routes, ips)


def _parse_ipv6_route(routes: str, ips: str) -> Dict[str, Dict[str, Set[str]]]:
    r = {}  # type: Dict[str, Dict[str, Set[str]]]
    route_p = re.compile(r'^(\S+) dev (\S+) proto (\S+) metric (\S+) .*pref (\S+)$')
    ip_p = re.compile(r'^\s+inet6 (\S+)/(.*)scope (.*)$')
    iface_p = re.compile(r'^(\d+): (\S+): (.*)$')
    for line in routes.splitlines():
        m = route_p.findall(line)
        if not m or m[0][0].lower() == 'default':
            continue
        net = m[0][0]
        if '/' not in net:  # aggregate /128 mask for single host sub-networks
            net += '/128'
        iface = m[0][1]
        if iface == 'lo':  # skip loopback devices
            continue
        if net not in r:
            r[net] = {}
        if iface not in r[net]:
            r[net][iface] = set()

    iface = None
    for line in ips.splitlines():
        m = ip_p.findall(line)
        if not m:
            m = iface_p.findall(line)
            if m:
                # drop @... suffix, if present
                iface = m[0][1].split('@')[0]
            continue
        ip = m[0][0]
        # find the network it belongs to
        net = [n for n in r.keys()
               if ipaddress.ip_address(ip) in ipaddress.ip_network(n)]
        if net and iface in r[net[0]]:
            assert iface
            r[net[0]][iface].add(ip)

    return r


def command_list_networks(ctx):
    # type: (CephadmContext) -> None
    r = list_networks(ctx)

    def serialize_sets(obj: Any) -> Any:
        return list(obj) if isinstance(obj, set) else obj

    print(json.dumps(r, indent=4, default=serialize_sets))

##################################


def command_ls(ctx):
    # type: (CephadmContext) -> None
    ls = list_daemons(ctx, detail=not ctx.no_detail,
                      legacy_dir=ctx.legacy_dir)
    print(json.dumps(ls, indent=4))


def with_units_to_int(v: str) -> int:
    if v.endswith('iB'):
        v = v[:-2]
    elif v.endswith('B'):
        v = v[:-1]
    mult = 1
    if v[-1].upper() == 'K':
        mult = 1024
        v = v[:-1]
    elif v[-1].upper() == 'M':
        mult = 1024 * 1024
        v = v[:-1]
    elif v[-1].upper() == 'G':
        mult = 1024 * 1024 * 1024
        v = v[:-1]
    elif v[-1].upper() == 'T':
        mult = 1024 * 1024 * 1024 * 1024
        v = v[:-1]
    return int(float(v) * mult)


def list_daemons(ctx, detail=True, legacy_dir=None):
    # type: (CephadmContext, bool, Optional[str]) -> List[Dict[str, str]]
    host_version: Optional[str] = None
    ls = []
    container_path = ctx.container_engine.path

    data_dir = ctx.data_dir
    if legacy_dir is not None:
        data_dir = os.path.abspath(legacy_dir + data_dir)

    # keep track of ceph versions we see
    seen_versions = {}  # type: Dict[str, Optional[str]]

    # keep track of image digests
    seen_digests = {}   # type: Dict[str, List[str]]

    # keep track of memory and cpu usage we've seen
    seen_memusage = {}  # type: Dict[str, int]
    seen_cpuperc = {}  # type: Dict[str, str]
    out, err, code = call(
        ctx,
        [container_path, 'stats', '--format', '{{.ID}},{{.MemUsage}}', '--no-stream'],
        verbosity=CallVerbosity.QUIET
    )
    seen_memusage_cid_len, seen_memusage = _parse_mem_usage(code, out)

    out, err, code = call(
        ctx,
        [container_path, 'stats', '--format', '{{.ID}},{{.CPUPerc}}', '--no-stream'],
        verbosity=CallVerbosity.QUIET
    )
    seen_cpuperc_cid_len, seen_cpuperc = _parse_cpu_perc(code, out)

    # /var/lib/ceph
    if os.path.exists(data_dir):
        for i in os.listdir(data_dir):
            if i in ['mon', 'osd', 'mds', 'mgr']:
                daemon_type = i
                for j in os.listdir(os.path.join(data_dir, i)):
                    if '-' not in j:
                        continue
                    (cluster, daemon_id) = j.split('-', 1)
                    fsid = get_legacy_daemon_fsid(ctx,
                                                  cluster, daemon_type, daemon_id,
                                                  legacy_dir=legacy_dir)
                    legacy_unit_name = 'ceph-%s@%s' % (daemon_type, daemon_id)
                    val: Dict[str, Any] = {
                        'style': 'legacy',
                        'name': '%s.%s' % (daemon_type, daemon_id),
                        'fsid': fsid if fsid is not None else 'unknown',
                        'systemd_unit': legacy_unit_name,
                    }
                    if detail:
                        (val['enabled'], val['state'], _) = check_unit(ctx, legacy_unit_name)
                        if not host_version:
                            try:
                                out, err, code = call(ctx,
                                                      ['ceph', '-v'],
                                                      verbosity=CallVerbosity.QUIET)
                                if not code and out.startswith('ceph version '):
                                    host_version = out.split(' ')[2]
                            except Exception:
                                pass
                        val['host_version'] = host_version
                    ls.append(val)
            elif is_fsid(i):
                fsid = str(i)  # convince mypy that fsid is a str here
                for j in os.listdir(os.path.join(data_dir, i)):
                    if '.' in j and os.path.isdir(os.path.join(data_dir, fsid, j)):
                        name = j
                        (daemon_type, daemon_id) = j.split('.', 1)
                        unit_name = get_unit_name(fsid,
                                                  daemon_type,
                                                  daemon_id)
                    else:
                        continue
                    val = {
                        'style': 'cephadm:v1',
                        'name': name,
                        'fsid': fsid,
                        'systemd_unit': unit_name,
                    }
                    if detail:
                        # get container id
                        (val['enabled'], val['state'], _) = check_unit(ctx, unit_name)
                        container_id = None
                        image_name = None
                        image_id = None
                        image_digests = None
                        version = None
                        start_stamp = None

                        out, err, code = get_container_stats(ctx, container_path, fsid, daemon_type, daemon_id)
                        if not code:
                            (container_id, image_name, image_id, start,
                             version) = out.strip().split(',')
                            image_id = normalize_container_id(image_id)
                            daemon_type = name.split('.', 1)[0]
                            start_stamp = try_convert_datetime(start)

                            # collect digests for this image id
                            image_digests = seen_digests.get(image_id)
                            if not image_digests:
                                out, err, code = call(
                                    ctx,
                                    [
                                        container_path, 'image', 'inspect', image_id,
                                        '--format', '{{.RepoDigests}}',
                                    ],
                                    verbosity=CallVerbosity.QUIET)
                                if not code:
                                    image_digests = list(set(map(
                                        normalize_image_digest,
                                        out.strip()[1:-1].split(' '))))
                                    seen_digests[image_id] = image_digests

                            # identify software version inside the container (if we can)
                            if not version or '.' not in version:
                                version = seen_versions.get(image_id, None)
                            if daemon_type == NFSGanesha.daemon_type:
                                version = NFSGanesha.get_version(ctx, container_id)
                            if daemon_type == CephIscsi.daemon_type:
                                version = CephIscsi.get_version(ctx, container_id)
                            if daemon_type == CephNvmeof.daemon_type:
                                version = CephNvmeof.get_version(ctx, container_id)
                            elif not version:
                                if daemon_type in Ceph.daemons:
                                    out, err, code = call(ctx,
                                                          [container_path, 'exec', container_id,
                                                           'ceph', '-v'],
                                                          verbosity=CallVerbosity.QUIET)
                                    if not code and \
                                       out.startswith('ceph version '):
                                        version = out.split(' ')[2]
                                        seen_versions[image_id] = version
                                elif daemon_type == 'grafana':
                                    out, err, code = call(ctx,
                                                          [container_path, 'exec', container_id,
                                                           'grafana-server', '-v'],
                                                          verbosity=CallVerbosity.QUIET)
                                    if not code and \
                                       out.startswith('Version '):
                                        version = out.split(' ')[1]
                                        seen_versions[image_id] = version
                                elif daemon_type in ['prometheus',
                                                     'alertmanager',
                                                     'node-exporter',
                                                     'loki',
                                                     'promtail']:
                                    version = Monitoring.get_version(ctx, container_id, daemon_type)
                                    seen_versions[image_id] = version
                                elif daemon_type == 'haproxy':
                                    out, err, code = call(ctx,
                                                          [container_path, 'exec', container_id,
                                                           'haproxy', '-v'],
                                                          verbosity=CallVerbosity.QUIET)
                                    if not code and \
                                       out.startswith('HA-Proxy version ') or \
                                       out.startswith('HAProxy version '):
                                        version = out.split(' ')[2]
                                        seen_versions[image_id] = version
                                elif daemon_type == 'keepalived':
                                    out, err, code = call(ctx,
                                                          [container_path, 'exec', container_id,
                                                           'keepalived', '--version'],
                                                          verbosity=CallVerbosity.QUIET)
                                    if not code and \
                                       err.startswith('Keepalived '):
                                        version = err.split(' ')[1]
                                        if version[0] == 'v':
                                            version = version[1:]
                                        seen_versions[image_id] = version
                                elif daemon_type == CustomContainer.daemon_type:
                                    # Because a custom container can contain
                                    # everything, we do not know which command
                                    # to execute to get the version.
                                    pass
                                elif daemon_type == SNMPGateway.daemon_type:
                                    version = SNMPGateway.get_version(ctx, fsid, daemon_id)
                                    seen_versions[image_id] = version
                                else:
                                    logger.warning('version for unknown daemon type %s' % daemon_type)
                        else:
                            vfile = os.path.join(data_dir, fsid, j, 'unit.image')  # type: ignore
                            try:
                                with open(vfile, 'r') as f:
                                    image_name = f.read().strip() or None
                            except IOError:
                                pass

                        # unit.meta?
                        mfile = os.path.join(data_dir, fsid, j, 'unit.meta')  # type: ignore
                        try:
                            with open(mfile, 'r') as f:
                                meta = json.loads(f.read())
                                val.update(meta)
                        except IOError:
                            pass

                        val['container_id'] = container_id
                        val['container_image_name'] = image_name
                        val['container_image_id'] = image_id
                        val['container_image_digests'] = image_digests
                        if container_id:
                            val['memory_usage'] = seen_memusage.get(container_id[0:seen_memusage_cid_len])
                            val['cpu_percentage'] = seen_cpuperc.get(container_id[0:seen_cpuperc_cid_len])
                        val['version'] = version
                        val['started'] = start_stamp
                        val['created'] = get_file_timestamp(
                            os.path.join(data_dir, fsid, j, 'unit.created')
                        )
                        val['deployed'] = get_file_timestamp(
                            os.path.join(data_dir, fsid, j, 'unit.image'))
                        val['configured'] = get_file_timestamp(
                            os.path.join(data_dir, fsid, j, 'unit.configured'))
                    ls.append(val)

    return ls


def _parse_mem_usage(code: int, out: str) -> Tuple[int, Dict[str, int]]:
    # keep track of memory usage we've seen
    seen_memusage = {}  # type: Dict[str, int]
    seen_memusage_cid_len = 0
    if not code:
        for line in out.splitlines():
            (cid, usage) = line.split(',')
            (used, limit) = usage.split(' / ')
            try:
                seen_memusage[cid] = with_units_to_int(used)
                if not seen_memusage_cid_len:
                    seen_memusage_cid_len = len(cid)
            except ValueError:
                logger.info('unable to parse memory usage line\n>{}'.format(line))
                pass
    return seen_memusage_cid_len, seen_memusage


def _parse_cpu_perc(code: int, out: str) -> Tuple[int, Dict[str, str]]:
    seen_cpuperc = {}
    seen_cpuperc_cid_len = 0
    if not code:
        for line in out.splitlines():
            (cid, cpuperc) = line.split(',')
            try:
                seen_cpuperc[cid] = cpuperc
                if not seen_cpuperc_cid_len:
                    seen_cpuperc_cid_len = len(cid)
            except ValueError:
                logger.info('unable to parse cpu percentage line\n>{}'.format(line))
                pass
    return seen_cpuperc_cid_len, seen_cpuperc


def get_daemon_description(ctx, fsid, name, detail=False, legacy_dir=None):
    # type: (CephadmContext, str, str, bool, Optional[str]) -> Dict[str, str]

    for d in list_daemons(ctx, detail=detail, legacy_dir=legacy_dir):
        if d['fsid'] != fsid:
            continue
        if d['name'] != name:
            continue
        return d
    raise Error('Daemon not found: {}. See `cephadm ls`'.format(name))


def get_container_stats(ctx: CephadmContext, container_path: str, fsid: str, daemon_type: str, daemon_id: str) -> Tuple[str, str, int]:
    c = CephContainer.for_daemon(ctx, fsid, daemon_type, daemon_id, 'bash')
    out, err, code = '', '', -1
    for name in (c.cname, c.old_cname):
        cmd = [
            container_path, 'inspect',
            '--format', '{{.Id}},{{.Config.Image}},{{.Image}},{{.Created}},{{index .Config.Labels "io.ceph.version"}}',
            name
        ]
        out, err, code = call(ctx, cmd, verbosity=CallVerbosity.QUIET)
        if not code:
            break
    return out, err, code

##################################


@default_image
def command_adopt(ctx):
    # type: (CephadmContext) -> None

    if not ctx.skip_pull:
        try:
            _pull_image(ctx, ctx.image)
        except UnauthorizedRegistryError:
            err_str = 'Failed to pull container image. Host may not be logged into container registry. Try `cephadm registry-login --registry-url <url> --registry-username <username> --registry-password <password>` or supply login info via a json file with `cephadm registry-login --registry-json <file>`'
            logger.debug(f'Pulling image for `command_adopt` failed: {err_str}')
            raise Error(err_str)

    (daemon_type, daemon_id) = ctx.name.split('.', 1)

    # legacy check
    if ctx.style != 'legacy':
        raise Error('adoption of style %s not implemented' % ctx.style)

    # lock
    fsid = get_legacy_daemon_fsid(ctx,
                                  ctx.cluster,
                                  daemon_type,
                                  daemon_id,
                                  legacy_dir=ctx.legacy_dir)
    if not fsid:
        raise Error('could not detect legacy fsid; set fsid in ceph.conf')
    lock = FileLock(ctx, fsid)
    lock.acquire()

    # call correct adoption
    if daemon_type in Ceph.daemons:
        command_adopt_ceph(ctx, daemon_type, daemon_id, fsid)
    elif daemon_type == 'prometheus':
        command_adopt_prometheus(ctx, daemon_id, fsid)
    elif daemon_type == 'grafana':
        command_adopt_grafana(ctx, daemon_id, fsid)
    elif daemon_type == 'node-exporter':
        raise Error('adoption of node-exporter not implemented')
    elif daemon_type == 'alertmanager':
        command_adopt_alertmanager(ctx, daemon_id, fsid)
    else:
        raise Error('daemon type %s not recognized' % daemon_type)


class AdoptOsd(object):
    def __init__(self, ctx, osd_data_dir, osd_id):
        # type: (CephadmContext, str, str) -> None
        self.ctx = ctx
        self.osd_data_dir = osd_data_dir
        self.osd_id = osd_id

    def check_online_osd(self):
        # type: () -> Tuple[Optional[str], Optional[str]]

        osd_fsid, osd_type = None, None

        path = os.path.join(self.osd_data_dir, 'fsid')
        try:
            with open(path, 'r') as f:
                osd_fsid = f.read().strip()
            logger.info('Found online OSD at %s' % path)
        except IOError:
            logger.info('Unable to read OSD fsid from %s' % path)
        if os.path.exists(os.path.join(self.osd_data_dir, 'type')):
            with open(os.path.join(self.osd_data_dir, 'type')) as f:
                osd_type = f.read().strip()
        else:
            logger.info('"type" file missing for OSD data dir')

        return osd_fsid, osd_type

    def check_offline_lvm_osd(self):
        # type: () -> Tuple[Optional[str], Optional[str]]
        osd_fsid, osd_type = None, None

        c = get_ceph_volume_container(
            self.ctx,
            args=['lvm', 'list', '--format=json'],
        )
        out, err, code = call_throws(self.ctx, c.run_cmd())
        if not code:
            try:
                js = json.loads(out)
                if self.osd_id in js:
                    logger.info('Found offline LVM OSD {}'.format(self.osd_id))
                    osd_fsid = js[self.osd_id][0]['tags']['ceph.osd_fsid']
                    for device in js[self.osd_id]:
                        if device['tags']['ceph.type'] == 'block':
                            osd_type = 'bluestore'
                            break
                        if device['tags']['ceph.type'] == 'data':
                            osd_type = 'filestore'
                            break
            except ValueError as e:
                logger.info('Invalid JSON in ceph-volume lvm list: {}'.format(e))

        return osd_fsid, osd_type

    def check_offline_simple_osd(self):
        # type: () -> Tuple[Optional[str], Optional[str]]
        osd_fsid, osd_type = None, None

        osd_file = glob('/etc/ceph/osd/{}-[a-f0-9-]*.json'.format(self.osd_id))
        if len(osd_file) == 1:
            with open(osd_file[0], 'r') as f:
                try:
                    js = json.loads(f.read())
                    logger.info('Found offline simple OSD {}'.format(self.osd_id))
                    osd_fsid = js['fsid']
                    osd_type = js['type']
                    if osd_type != 'filestore':
                        # need this to be mounted for the adopt to work, as it
                        # needs to move files from this directory
                        call_throws(self.ctx, ['mount', js['data']['path'], self.osd_data_dir])
                except ValueError as e:
                    logger.info('Invalid JSON in {}: {}'.format(osd_file, e))

        return osd_fsid, osd_type

    def change_cluster_name(self) -> None:
        logger.info('Attempting to convert osd cluster name to ceph . . .')
        c = get_ceph_volume_container(
            self.ctx,
            args=['lvm', 'list', '{}'.format(self.osd_id), '--format=json'],
        )
        out, err, code = call_throws(self.ctx, c.run_cmd())
        if code:
            raise Exception(f'Failed to get list of LVs: {err}\nceph-volume failed with rc {code}')
        try:
            js = json.loads(out)
            if not js:
                raise RuntimeError(f'Failed to find osd.{self.osd_id}')
            device: Optional[Dict[Any, Any]] = None
            for d in js[self.osd_id]:
                if d['type'] == 'block':
                    device = d
                    break
            if not device:
                raise RuntimeError(f'Failed to find block device for osd.{self.osd_id}')
            vg = device['vg_name']
            out, err, code = call_throws(self.ctx, ['lvchange', '--deltag', f'ceph.cluster_name={self.ctx.cluster}', vg])
            if code:
                raise RuntimeError(f"Can't delete tag ceph.cluster_name={self.ctx.cluster} on osd.{self.osd_id}.\nlvchange failed with rc {code}")
            out, err, code = call_throws(self.ctx, ['lvchange', '--addtag', 'ceph.cluster_name=ceph', vg])
            if code:
                raise RuntimeError(f"Can't add tag ceph.cluster_name=ceph on osd.{self.osd_id}.\nlvchange failed with rc {code}")
            logger.info('Successfully converted osd cluster name')
        except (Exception, RuntimeError) as e:
            logger.info(f'Failed to convert osd cluster name: {e}')


def command_adopt_ceph(ctx, daemon_type, daemon_id, fsid):
    # type: (CephadmContext, str, str, str) -> None

    (uid, gid) = extract_uid_gid(ctx)

    data_dir_src = ('/var/lib/ceph/%s/%s-%s' %
                    (daemon_type, ctx.cluster, daemon_id))
    data_dir_src = os.path.abspath(ctx.legacy_dir + data_dir_src)

    if not os.path.exists(data_dir_src):
        raise Error("{}.{} data directory '{}' does not exist.  "
                    'Incorrect ID specified, or daemon already adopted?'.format(
                        daemon_type, daemon_id, data_dir_src))

    osd_fsid = None
    if daemon_type == 'osd':
        adopt_osd = AdoptOsd(ctx, data_dir_src, daemon_id)
        osd_fsid, osd_type = adopt_osd.check_online_osd()
        if not osd_fsid:
            osd_fsid, osd_type = adopt_osd.check_offline_lvm_osd()
        if not osd_fsid:
            osd_fsid, osd_type = adopt_osd.check_offline_simple_osd()
        if not osd_fsid:
            raise Error('Unable to find OSD {}'.format(daemon_id))
        elif ctx.cluster != 'ceph':
            adopt_osd.change_cluster_name()
        logger.info('objectstore_type is %s' % osd_type)
        assert osd_type
        if osd_type == 'filestore':
            raise Error('FileStore is not supported by cephadm')

    # NOTE: implicit assumption here that the units correspond to the
    # cluster we are adopting based on the /etc/{defaults,sysconfig}/ceph
    # CLUSTER field.
    unit_name = 'ceph-%s@%s' % (daemon_type, daemon_id)
    (enabled, state, _) = check_unit(ctx, unit_name)
    if state == 'running':
        logger.info('Stopping old systemd unit %s...' % unit_name)
        call_throws(ctx, ['systemctl', 'stop', unit_name])
    if enabled:
        logger.info('Disabling old systemd unit %s...' % unit_name)
        call_throws(ctx, ['systemctl', 'disable', unit_name])

    # data
    logger.info('Moving data...')
    data_dir_dst = make_data_dir(ctx, fsid, daemon_type, daemon_id,
                                 uid=uid, gid=gid)
    move_files(ctx, glob(os.path.join(data_dir_src, '*')),
               data_dir_dst,
               uid=uid, gid=gid)
    logger.debug('Remove dir `%s`' % (data_dir_src))
    if os.path.ismount(data_dir_src):
        call_throws(ctx, ['umount', data_dir_src])
    os.rmdir(data_dir_src)

    logger.info('Chowning content...')
    call_throws(ctx, ['chown', '-c', '-R', '%d.%d' % (uid, gid), data_dir_dst])

    if daemon_type == 'mon':
        # rename *.ldb -> *.sst, in case they are coming from ubuntu
        store = os.path.join(data_dir_dst, 'store.db')
        num_renamed = 0
        if os.path.exists(store):
            for oldf in os.listdir(store):
                if oldf.endswith('.ldb'):
                    newf = oldf.replace('.ldb', '.sst')
                    oldp = os.path.join(store, oldf)
                    newp = os.path.join(store, newf)
                    logger.debug('Renaming %s -> %s' % (oldp, newp))
                    os.rename(oldp, newp)
        if num_renamed:
            logger.info('Renamed %d leveldb *.ldb files to *.sst',
                        num_renamed)
    if daemon_type == 'osd':
        for n in ['block', 'block.db', 'block.wal']:
            p = os.path.join(data_dir_dst, n)
            if os.path.exists(p):
                logger.info('Chowning %s...' % p)
                os.chown(p, uid, gid)
        # disable the ceph-volume 'simple' mode files on the host
        simple_fn = os.path.join('/etc/ceph/osd',
                                 '%s-%s.json' % (daemon_id, osd_fsid))
        if os.path.exists(simple_fn):
            new_fn = simple_fn + '.adopted-by-cephadm'
            logger.info('Renaming %s -> %s', simple_fn, new_fn)
            os.rename(simple_fn, new_fn)
            logger.info('Disabling host unit ceph-volume@ simple unit...')
            call(ctx, ['systemctl', 'disable',
                       'ceph-volume@simple-%s-%s.service' % (daemon_id, osd_fsid)])
        else:
            # assume this is an 'lvm' c-v for now, but don't error
            # out if it's not.
            logger.info('Disabling host unit ceph-volume@ lvm unit...')
            call(ctx, ['systemctl', 'disable',
                       'ceph-volume@lvm-%s-%s.service' % (daemon_id, osd_fsid)])

    # config
    config_src = '/etc/ceph/%s.conf' % (ctx.cluster)
    config_src = os.path.abspath(ctx.legacy_dir + config_src)
    config_dst = os.path.join(data_dir_dst, 'config')
    copy_files(ctx, [config_src], config_dst, uid=uid, gid=gid)

    # logs
    logger.info('Moving logs...')
    log_dir_src = ('/var/log/ceph/%s-%s.%s.log*' %
                   (ctx.cluster, daemon_type, daemon_id))
    log_dir_src = os.path.abspath(ctx.legacy_dir + log_dir_src)
    log_dir_dst = make_log_dir(ctx, fsid, uid=uid, gid=gid)
    move_files(ctx, glob(log_dir_src),
               log_dir_dst,
               uid=uid, gid=gid)

    logger.info('Creating new units...')
    make_var_run(ctx, fsid, uid, gid)
    c = get_container(ctx, fsid, daemon_type, daemon_id)
    deploy_daemon_units(ctx, fsid, uid, gid, daemon_type, daemon_id, c,
                        enable=True,  # unconditionally enable the new unit
                        start=(state == 'running' or ctx.force_start),
                        osd_fsid=osd_fsid)
    update_firewalld(ctx, daemon_type)


def command_adopt_prometheus(ctx, daemon_id, fsid):
    # type: (CephadmContext, str, str) -> None
    daemon_type = 'prometheus'
    (uid, gid) = extract_uid_gid_monitoring(ctx, daemon_type)
    # should try to set the ports we know cephadm defaults
    # to for these services in the firewall.
    ports = Monitoring.port_map['prometheus']
    endpoints = [EndPoint('0.0.0.0', p) for p in ports]

    _stop_and_disable(ctx, 'prometheus')

    data_dir_dst = make_data_dir(ctx, fsid, daemon_type, daemon_id,
                                 uid=uid, gid=gid)

    # config
    config_src = '/etc/prometheus/prometheus.yml'
    config_src = os.path.abspath(ctx.legacy_dir + config_src)
    config_dst = os.path.join(data_dir_dst, 'etc/prometheus')
    makedirs(config_dst, uid, gid, 0o755)
    copy_files(ctx, [config_src], config_dst, uid=uid, gid=gid)

    # data
    data_src = '/var/lib/prometheus/metrics/'
    data_src = os.path.abspath(ctx.legacy_dir + data_src)
    data_dst = os.path.join(data_dir_dst, 'data')
    copy_tree(ctx, [data_src], data_dst, uid=uid, gid=gid)

    make_var_run(ctx, fsid, uid, gid)
    c = get_container(ctx, fsid, daemon_type, daemon_id)
    deploy_daemon(ctx, fsid, daemon_type, daemon_id, c, uid, gid,
                  deployment_type=DeploymentType.REDEPLOY, endpoints=endpoints)
    update_firewalld(ctx, daemon_type)


def command_adopt_grafana(ctx, daemon_id, fsid):
    # type: (CephadmContext, str, str) -> None

    daemon_type = 'grafana'
    (uid, gid) = extract_uid_gid_monitoring(ctx, daemon_type)
    # should try to set the ports we know cephadm defaults
    # to for these services in the firewall.
    ports = Monitoring.port_map['grafana']
    endpoints = [EndPoint('0.0.0.0', p) for p in ports]

    _stop_and_disable(ctx, 'grafana-server')

    data_dir_dst = make_data_dir(ctx, fsid, daemon_type, daemon_id,
                                 uid=uid, gid=gid)

    # config
    config_src = '/etc/grafana/grafana.ini'
    config_src = os.path.abspath(ctx.legacy_dir + config_src)
    config_dst = os.path.join(data_dir_dst, 'etc/grafana')
    makedirs(config_dst, uid, gid, 0o755)
    copy_files(ctx, [config_src], config_dst, uid=uid, gid=gid)

    prov_src = '/etc/grafana/provisioning/'
    prov_src = os.path.abspath(ctx.legacy_dir + prov_src)
    prov_dst = os.path.join(data_dir_dst, 'etc/grafana')
    copy_tree(ctx, [prov_src], prov_dst, uid=uid, gid=gid)

    # cert
    cert = '/etc/grafana/grafana.crt'
    key = '/etc/grafana/grafana.key'
    if os.path.exists(cert) and os.path.exists(key):
        cert_src = '/etc/grafana/grafana.crt'
        cert_src = os.path.abspath(ctx.legacy_dir + cert_src)
        makedirs(os.path.join(data_dir_dst, 'etc/grafana/certs'), uid, gid, 0o755)
        cert_dst = os.path.join(data_dir_dst, 'etc/grafana/certs/cert_file')
        copy_files(ctx, [cert_src], cert_dst, uid=uid, gid=gid)

        key_src = '/etc/grafana/grafana.key'
        key_src = os.path.abspath(ctx.legacy_dir + key_src)
        key_dst = os.path.join(data_dir_dst, 'etc/grafana/certs/cert_key')
        copy_files(ctx, [key_src], key_dst, uid=uid, gid=gid)

        _adjust_grafana_ini(os.path.join(config_dst, 'grafana.ini'))
    else:
        logger.debug('Skipping ssl, missing cert {} or key {}'.format(cert, key))

    # data - possible custom dashboards/plugins
    data_src = '/var/lib/grafana/'
    data_src = os.path.abspath(ctx.legacy_dir + data_src)
    data_dst = os.path.join(data_dir_dst, 'data')
    copy_tree(ctx, [data_src], data_dst, uid=uid, gid=gid)

    make_var_run(ctx, fsid, uid, gid)
    c = get_container(ctx, fsid, daemon_type, daemon_id)
    deploy_daemon(ctx, fsid, daemon_type, daemon_id, c, uid, gid,
                  deployment_type=DeploymentType.REDEPLOY, endpoints=endpoints)
    update_firewalld(ctx, daemon_type)


def command_adopt_alertmanager(ctx, daemon_id, fsid):
    # type: (CephadmContext, str, str) -> None

    daemon_type = 'alertmanager'
    (uid, gid) = extract_uid_gid_monitoring(ctx, daemon_type)
    # should try to set the ports we know cephadm defaults
    # to for these services in the firewall.
    ports = Monitoring.port_map['alertmanager']
    endpoints = [EndPoint('0.0.0.0', p) for p in ports]

    _stop_and_disable(ctx, 'prometheus-alertmanager')

    data_dir_dst = make_data_dir(ctx, fsid, daemon_type, daemon_id,
                                 uid=uid, gid=gid)

    # config
    config_src = '/etc/prometheus/alertmanager.yml'
    config_src = os.path.abspath(ctx.legacy_dir + config_src)
    config_dst = os.path.join(data_dir_dst, 'etc/alertmanager')
    makedirs(config_dst, uid, gid, 0o755)
    copy_files(ctx, [config_src], config_dst, uid=uid, gid=gid)

    # data
    data_src = '/var/lib/prometheus/alertmanager/'
    data_src = os.path.abspath(ctx.legacy_dir + data_src)
    data_dst = os.path.join(data_dir_dst, 'etc/alertmanager/data')
    copy_tree(ctx, [data_src], data_dst, uid=uid, gid=gid)

    make_var_run(ctx, fsid, uid, gid)
    c = get_container(ctx, fsid, daemon_type, daemon_id)
    deploy_daemon(ctx, fsid, daemon_type, daemon_id, c, uid, gid,
                  deployment_type=DeploymentType.REDEPLOY, endpoints=endpoints)
    update_firewalld(ctx, daemon_type)


def _adjust_grafana_ini(filename):
    # type: (str) -> None

    # Update cert_file, cert_key pathnames in server section
    # ConfigParser does not preserve comments
    try:
        with open(filename, 'r') as grafana_ini:
            lines = grafana_ini.readlines()
        with write_new(filename, perms=None) as grafana_ini:
            server_section = False
            for line in lines:
                if line.startswith('['):
                    server_section = False
                if line.startswith('[server]'):
                    server_section = True
                if server_section:
                    line = re.sub(r'^cert_file.*',
                                  'cert_file = /etc/grafana/certs/cert_file', line)
                    line = re.sub(r'^cert_key.*',
                                  'cert_key = /etc/grafana/certs/cert_key', line)
                grafana_ini.write(line)
    except OSError as err:
        raise Error('Cannot update {}: {}'.format(filename, err))


def _stop_and_disable(ctx, unit_name):
    # type: (CephadmContext, str) -> None

    (enabled, state, _) = check_unit(ctx, unit_name)
    if state == 'running':
        logger.info('Stopping old systemd unit %s...' % unit_name)
        call_throws(ctx, ['systemctl', 'stop', unit_name])
    if enabled:
        logger.info('Disabling old systemd unit %s...' % unit_name)
        call_throws(ctx, ['systemctl', 'disable', unit_name])

##################################


def command_rm_daemon(ctx):
    # type: (CephadmContext) -> None
    lock = FileLock(ctx, ctx.fsid)
    lock.acquire()

    (daemon_type, daemon_id) = ctx.name.split('.', 1)
    unit_name = get_unit_name_by_daemon_name(ctx, ctx.fsid, ctx.name)

    if daemon_type in ['mon', 'osd'] and not ctx.force:
        raise Error('must pass --force to proceed: '
                    'this command may destroy precious data!')

    call(ctx, ['systemctl', 'stop', unit_name],
         verbosity=CallVerbosity.DEBUG)
    call(ctx, ['systemctl', 'reset-failed', unit_name],
         verbosity=CallVerbosity.DEBUG)
    call(ctx, ['systemctl', 'disable', unit_name],
         verbosity=CallVerbosity.DEBUG)

    # force remove rgw admin socket file if leftover
    if daemon_type in ['rgw']:
        rgw_asok_path = f'/var/run/ceph/{ctx.fsid}/ceph-client.{ctx.name}.*.asok'
        call(ctx, ['rm', '-rf', rgw_asok_path],
             verbosity=CallVerbosity.DEBUG)

    data_dir = get_data_dir(ctx.fsid, ctx.data_dir, daemon_type, daemon_id)
    if daemon_type in ['mon', 'osd', 'prometheus'] and \
       not ctx.force_delete_data:
        # rename it out of the way -- do not delete
        backup_dir = os.path.join(ctx.data_dir, ctx.fsid, 'removed')
        if not os.path.exists(backup_dir):
            makedirs(backup_dir, 0, 0, DATA_DIR_MODE)
        dirname = '%s.%s_%s' % (daemon_type, daemon_id,
                                datetime.datetime.utcnow().strftime(DATEFMT))
        os.rename(data_dir,
                  os.path.join(backup_dir, dirname))
    else:
        call_throws(ctx, ['rm', '-rf', data_dir])

    endpoints = fetch_tcp_ports(ctx)
    ports: List[int] = [e.port for e in endpoints]
    if ports:
        try:
            fw = Firewalld(ctx)
            fw.close_ports(ports)
            fw.apply_rules()
        except RuntimeError as e:
            # in case we cannot close the ports we will remove
            # the daemon but keep them open.
            logger.warning(f' Error when trying to close ports: {e}')


##################################


def _zap(ctx: CephadmContext, what: str) -> None:
    mounts = get_container_mounts(ctx, ctx.fsid, 'clusterless-ceph-volume', None)
    c = get_ceph_volume_container(ctx,
                                  args=['lvm', 'zap', '--destroy', what],
                                  volume_mounts=mounts,
                                  envs=ctx.env)
    logger.info(f'Zapping {what}...')
    out, err, code = call_throws(ctx, c.run_cmd())


@infer_image
def _zap_osds(ctx: CephadmContext) -> None:
    # assume fsid lock already held

    # list
    mounts = get_container_mounts(ctx, ctx.fsid, 'clusterless-ceph-volume', None)
    c = get_ceph_volume_container(ctx,
                                  args=['inventory', '--format', 'json'],
                                  volume_mounts=mounts,
                                  envs=ctx.env)
    out, err, code = call_throws(ctx, c.run_cmd())
    if code:
        raise Error('failed to list osd inventory')
    try:
        ls = json.loads(out)
    except ValueError as e:
        raise Error(f'Invalid JSON in ceph-volume inventory: {e}')

    for i in ls:
        matches = [lv.get('cluster_fsid') == ctx.fsid and i.get('ceph_device') for lv in i.get('lvs', [])]
        if any(matches) and all(matches):
            _zap(ctx, i.get('path'))
        elif any(matches):
            lv_names = [lv['name'] for lv in i.get('lvs', [])]
            # TODO: we need to map the lv_names back to device paths (the vg
            # id isn't part of the output here!)
            logger.warning(f'Not zapping LVs (not implemented): {lv_names}')


def command_zap_osds(ctx: CephadmContext) -> None:
    if not ctx.force:
        raise Error('must pass --force to proceed: '
                    'this command may destroy precious data!')

    lock = FileLock(ctx, ctx.fsid)
    lock.acquire()

    _zap_osds(ctx)

##################################


def get_ceph_cluster_count(ctx: CephadmContext) -> int:
    return len([c for c in os.listdir(ctx.data_dir) if is_fsid(c)])


def command_rm_cluster(ctx: CephadmContext) -> None:
    if not ctx.force:
        raise Error('must pass --force to proceed: '
                    'this command may destroy precious data!')

    lock = FileLock(ctx, ctx.fsid)
    lock.acquire()
    _rm_cluster(ctx, ctx.keep_logs, ctx.zap_osds)


def _rm_cluster(ctx: CephadmContext, keep_logs: bool, zap_osds: bool) -> None:

    if not ctx.fsid:
        raise Error('must select the cluster to delete by passing --fsid to proceed')

    def disable_systemd_service(unit_name: str) -> None:
        call(ctx, ['systemctl', 'stop', unit_name],
             verbosity=CallVerbosity.DEBUG)
        call(ctx, ['systemctl', 'reset-failed', unit_name],
             verbosity=CallVerbosity.DEBUG)
        call(ctx, ['systemctl', 'disable', unit_name],
             verbosity=CallVerbosity.DEBUG)

    logger.info(f'Deleting cluster with fsid: {ctx.fsid}')

    # stop + disable individual daemon units
    for d in list_daemons(ctx, detail=False):
        if d['fsid'] != ctx.fsid:
            continue
        if d['style'] != 'cephadm:v1':
            continue
        disable_systemd_service(get_unit_name(ctx.fsid, d['name']))

    # cluster units
    for unit_name in ['ceph-%s.target' % ctx.fsid]:
        disable_systemd_service(unit_name)

    slice_name = 'system-ceph\\x2d{}.slice'.format(ctx.fsid.replace('-', '\\x2d'))
    call(ctx, ['systemctl', 'stop', slice_name],
         verbosity=CallVerbosity.DEBUG)

    # osds?
    if zap_osds:
        _zap_osds(ctx)

    # rm units
    call_throws(ctx, ['rm', '-f', ctx.unit_dir
                      + '/ceph-%s@.service' % ctx.fsid])
    call_throws(ctx, ['rm', '-f', ctx.unit_dir
                      + '/ceph-%s.target' % ctx.fsid])
    call_throws(ctx, ['rm', '-rf',
                      ctx.unit_dir + '/ceph-%s.target.wants' % ctx.fsid])
    # rm data
    call_throws(ctx, ['rm', '-rf', ctx.data_dir + '/' + ctx.fsid])

    if not keep_logs:
        # rm logs
        call_throws(ctx, ['rm', '-rf', ctx.log_dir + '/' + ctx.fsid])
        call_throws(ctx, ['rm', '-rf', ctx.log_dir
                          + '/*.wants/ceph-%s@*' % ctx.fsid])

    # rm logrotate config
    call_throws(ctx, ['rm', '-f', ctx.logrotate_dir + '/ceph-%s' % ctx.fsid])

    # if last cluster on host remove shared files
    if get_ceph_cluster_count(ctx) == 0:
        disable_systemd_service('ceph.target')

        # rm shared ceph target files
        call_throws(ctx, ['rm', '-f', ctx.unit_dir + '/multi-user.target.wants/ceph.target'])
        call_throws(ctx, ['rm', '-f', ctx.unit_dir + '/ceph.target'])

        # rm cephadm logrotate config
        call_throws(ctx, ['rm', '-f', ctx.logrotate_dir + '/cephadm'])

        if not keep_logs:
            # remove all cephadm logs
            for fname in glob(f'{ctx.log_dir}/cephadm.log*'):
                os.remove(fname)

    # rm sysctl settings
    sysctl_dirs: List[Path] = [Path(ctx.sysctl_dir), Path('/usr/lib/sysctl.d')]

    for sysctl_dir in sysctl_dirs:
        for p in sysctl_dir.glob(f'90-ceph-{ctx.fsid}-*.conf'):
            p.unlink()

    # cleanup remaining ceph directories
    ceph_dirs = [f'/run/ceph/{ctx.fsid}', f'/tmp/cephadm-{ctx.fsid}', f'/var/run/ceph/{ctx.fsid}']
    for dd in ceph_dirs:
        shutil.rmtree(dd, ignore_errors=True)

    # clean up config, keyring, and pub key files
    files = [CEPH_DEFAULT_CONF, CEPH_DEFAULT_PUBKEY, CEPH_DEFAULT_KEYRING]
    if os.path.exists(files[0]):
        valid_fsid = False
        with open(files[0]) as f:
            if ctx.fsid in f.read():
                valid_fsid = True
        if valid_fsid:
            # rm configuration files on /etc/ceph
            for n in range(0, len(files)):
                if os.path.exists(files[n]):
                    os.remove(files[n])

##################################


def check_time_sync(ctx, enabler=None):
    # type: (CephadmContext, Optional[Packager]) -> bool
    units = [
        'chrony.service',  # 18.04 (at least)
        'chronyd.service',  # el / opensuse
        'systemd-timesyncd.service',
        'ntpd.service',  # el7 (at least)
        'ntp.service',  # 18.04 (at least)
        'ntpsec.service',  # 20.04 (at least) / buster
        'openntpd.service',  # ubuntu / debian
    ]
    if not check_units(ctx, units, enabler):
        logger.warning('No time sync service is running; checked for %s' % units)
        return False
    return True


def command_check_host(ctx: CephadmContext) -> None:
    errors = []
    commands = ['systemctl', 'lvcreate']

    try:
        engine = check_container_engine(ctx)
        logger.info(f'{engine} is present')
    except Error as e:
        errors.append(str(e))

    for command in commands:
        try:
            find_program(command)
            logger.info('%s is present' % command)
        except ValueError:
            errors.append('%s binary does not appear to be installed' % command)

    # check for configured+running chronyd or ntp
    if not check_time_sync(ctx):
        errors.append('No time synchronization is active')

    if 'expect_hostname' in ctx and ctx.expect_hostname:
        if get_hostname().lower() != ctx.expect_hostname.lower():
            errors.append('hostname "%s" does not match expected hostname "%s"' % (
                get_hostname(), ctx.expect_hostname))
        else:
            logger.info('Hostname "%s" matches what is expected.',
                        ctx.expect_hostname)

    if errors:
        raise Error('\nERROR: '.join(errors))

    logger.info('Host looks OK')

##################################


def get_ssh_vars(ssh_user: str) -> Tuple[int, int, str]:
    try:
        s_pwd = pwd.getpwnam(ssh_user)
    except KeyError:
        raise Error('Cannot find uid/gid for ssh-user: %s' % (ssh_user))

    ssh_uid = s_pwd.pw_uid
    ssh_gid = s_pwd.pw_gid
    ssh_dir = os.path.join(s_pwd.pw_dir, '.ssh')
    return ssh_uid, ssh_gid, ssh_dir


def authorize_ssh_key(ssh_pub_key: str, ssh_user: str) -> bool:
    """Authorize the public key for the provided ssh user"""

    def key_in_file(path: str, key: str) -> bool:
        if not os.path.exists(path):
            return False
        with open(path) as f:
            lines = f.readlines()
            for line in lines:
                if line.strip() == key.strip():
                    return True
        return False

    logger.info(f'Adding key to {ssh_user}@localhost authorized_keys...')
    if ssh_pub_key is None or ssh_pub_key.isspace():
        raise Error('Trying to authorize an empty ssh key')

    ssh_pub_key = ssh_pub_key.strip()
    ssh_uid, ssh_gid, ssh_dir = get_ssh_vars(ssh_user)
    if not os.path.exists(ssh_dir):
        makedirs(ssh_dir, ssh_uid, ssh_gid, 0o700)

    auth_keys_file = '%s/authorized_keys' % ssh_dir
    if key_in_file(auth_keys_file, ssh_pub_key):
        logger.info(f'key already in {ssh_user}@localhost authorized_keys...')
        return False

    add_newline = False
    if os.path.exists(auth_keys_file):
        with open(auth_keys_file, 'r') as f:
            f.seek(0, os.SEEK_END)
            if f.tell() > 0:
                f.seek(f.tell() - 1, os.SEEK_SET)  # go to last char
                if f.read() != '\n':
                    add_newline = True

    with open(auth_keys_file, 'a') as f:
        os.fchown(f.fileno(), ssh_uid, ssh_gid)  # just in case we created it
        os.fchmod(f.fileno(), DEFAULT_MODE)  # just in case we created it
        if add_newline:
            f.write('\n')
        f.write(ssh_pub_key + '\n')

    return True


def revoke_ssh_key(key: str, ssh_user: str) -> None:
    """Revoke the public key authorization for the ssh user"""
    ssh_uid, ssh_gid, ssh_dir = get_ssh_vars(ssh_user)
    auth_keys_file = '%s/authorized_keys' % ssh_dir
    deleted = False
    if os.path.exists(auth_keys_file):
        with open(auth_keys_file, 'r') as f:
            lines = f.readlines()
        _, filename = tempfile.mkstemp()
        with open(filename, 'w') as f:
            os.fchown(f.fileno(), ssh_uid, ssh_gid)
            os.fchmod(f.fileno(), DEFAULT_MODE)  # secure access to the keys file
            for line in lines:
                if line.strip() == key.strip():
                    deleted = True
                else:
                    f.write(line)

    if deleted:
        shutil.move(filename, auth_keys_file)
    else:
        logger.warning('Cannot find the ssh key to be deleted')


def check_ssh_connectivity(ctx: CephadmContext) -> None:

    def cmd_is_available(cmd: str) -> bool:
        if shutil.which(cmd) is None:
            logger.warning(f'Command not found: {cmd}')
            return False
        return True

    if not cmd_is_available('ssh') or not cmd_is_available('ssh-keygen'):
        logger.warning('Cannot check ssh connectivity. Skipping...')
        return

    ssh_priv_key_path = ''
    ssh_pub_key_path = ''
    ssh_signed_cert_path = ''
    if ctx.ssh_private_key and ctx.ssh_public_key:
        # let's use the keys provided by the user
        ssh_priv_key_path = pathify(ctx.ssh_private_key.name)
        ssh_pub_key_path = pathify(ctx.ssh_public_key.name)
    elif ctx.ssh_private_key and ctx.ssh_signed_cert:
        # CA signed keys use case
        ssh_priv_key_path = pathify(ctx.ssh_private_key.name)
        ssh_signed_cert_path = pathify(ctx.ssh_signed_cert.name)
    else:
        # no custom keys, let's generate some random keys just for this check
        ssh_priv_key_path = f'/tmp/ssh_key_{uuid.uuid1()}'
        ssh_pub_key_path = f'{ssh_priv_key_path}.pub'
        ssh_key_gen_cmd = ['ssh-keygen', '-q', '-t', 'rsa', '-N', '', '-C', '', '-f', ssh_priv_key_path]
        _, _, code = call(ctx, ssh_key_gen_cmd)
        if code != 0:
            logger.warning('Cannot generate keys to check ssh connectivity.')
            return

    if ssh_signed_cert_path:
        logger.info('Verification for CA signed keys authentication not implemented. Skipping ...')
    elif ssh_pub_key_path:
        logger.info('Verifying ssh connectivity using standard pubkey authentication ...')
        with open(ssh_pub_key_path, 'r') as f:
            key = f.read().strip()
        new_key = authorize_ssh_key(key, ctx.ssh_user)
        ssh_cfg_file_arg = ['-F', pathify(ctx.ssh_config.name)] if ctx.ssh_config else []
        _, _, code = call(ctx, ['ssh', '-o StrictHostKeyChecking=no',
                                *ssh_cfg_file_arg, '-i', ssh_priv_key_path,
                                '-o PasswordAuthentication=no',
                                f'{ctx.ssh_user}@{get_hostname()}',
                                'sudo echo'])

        # we only remove the key if it's a new one. In case the user has provided
        # some already existing key then we don't alter authorized_keys file
        if new_key:
            revoke_ssh_key(key, ctx.ssh_user)

        pub_key_msg = '- The public key file configured by --ssh-public-key is valid\n' if ctx.ssh_public_key else ''
        prv_key_msg = '- The private key file configured by --ssh-private-key is valid\n' if ctx.ssh_private_key else ''
        ssh_cfg_msg = '- The ssh configuration file configured by --ssh-config is valid\n' if ctx.ssh_config else ''
        err_msg = f"""
** Please verify your user's ssh configuration and make sure:
- User {ctx.ssh_user} must have passwordless sudo access
{pub_key_msg}{prv_key_msg}{ssh_cfg_msg}
"""
        if code != 0:
            raise Error(err_msg)


def command_prepare_host(ctx: CephadmContext) -> None:
    logger.info('Verifying podman|docker is present...')
    pkg = None
    try:
        check_container_engine(ctx)
    except Error as e:
        logger.warning(str(e))
        if not pkg:
            pkg = create_packager(ctx)
        pkg.install_podman()

    logger.info('Verifying lvm2 is present...')
    if not find_executable('lvcreate'):
        if not pkg:
            pkg = create_packager(ctx)
        pkg.install(['lvm2'])

    logger.info('Verifying time synchronization is in place...')
    if not check_time_sync(ctx):
        if not pkg:
            pkg = create_packager(ctx)
        pkg.install(['chrony'])
        # check again, and this time try to enable
        # the service
        check_time_sync(ctx, enabler=pkg)

    if 'expect_hostname' in ctx and ctx.expect_hostname and ctx.expect_hostname != get_hostname():
        logger.warning('Adjusting hostname from %s -> %s...' % (get_hostname(), ctx.expect_hostname))
        call_throws(ctx, ['hostname', ctx.expect_hostname])
        with open('/etc/hostname', 'w') as f:
            f.write(ctx.expect_hostname + '\n')

    logger.info('Repeating the final host check...')
    command_check_host(ctx)

##################################


class CustomValidation(argparse.Action):

    def _check_name(self, values: str) -> None:
        try:
            (daemon_type, daemon_id) = values.split('.', 1)
        except ValueError:
            raise argparse.ArgumentError(self,
                                         'must be of the format <type>.<id>. For example, osd.1 or prometheus.myhost.com')

        daemons = get_supported_daemons()
        if daemon_type not in daemons:
            raise argparse.ArgumentError(self,
                                         'name must declare the type of daemon e.g. '
                                         '{}'.format(', '.join(daemons)))

    def __call__(self, parser: argparse.ArgumentParser, namespace: argparse.Namespace, values: Union[str, Sequence[Any], None],
                 option_string: Optional[str] = None) -> None:
        assert isinstance(values, str)
        if self.dest == 'name':
            self._check_name(values)
            setattr(namespace, self.dest, values)

##################################


def get_distro():
    # type: () -> Tuple[Optional[str], Optional[str], Optional[str]]
    distro = None
    distro_version = None
    distro_codename = None
    with open('/etc/os-release', 'r') as f:
        for line in f.readlines():
            line = line.strip()
            if '=' not in line or line.startswith('#'):
                continue
            (var, val) = line.split('=', 1)
            if val[0] == '"' and val[-1] == '"':
                val = val[1:-1]
            if var == 'ID':
                distro = val.lower()
            elif var == 'VERSION_ID':
                distro_version = val.lower()
            elif var == 'VERSION_CODENAME':
                distro_codename = val.lower()
    return distro, distro_version, distro_codename


class Packager(object):
    def __init__(self, ctx: CephadmContext,
                 stable: Optional[str] = None, version: Optional[str] = None,
                 branch: Optional[str] = None, commit: Optional[str] = None):
        assert \
            (stable and not version and not branch and not commit) or \
            (not stable and version and not branch and not commit) or \
            (not stable and not version and branch) or \
            (not stable and not version and not branch and not commit)
        self.ctx = ctx
        self.stable = stable
        self.version = version
        self.branch = branch
        self.commit = commit

    def validate(self) -> None:
        """Validate parameters before writing any state to disk."""
        pass

    def add_repo(self) -> None:
        raise NotImplementedError

    def rm_repo(self) -> None:
        raise NotImplementedError

    def install(self, ls: List[str]) -> None:
        raise NotImplementedError

    def install_podman(self) -> None:
        raise NotImplementedError

    def query_shaman(self, distro: str, distro_version: Any, branch: Optional[str], commit: Optional[str]) -> str:
        # query shaman
        logger.info('Fetching repo metadata from shaman and chacra...')
        shaman_url = 'https://shaman.ceph.com/api/repos/ceph/{branch}/{sha1}/{distro}/{distro_version}/repo/?arch={arch}'.format(
            distro=distro,
            distro_version=distro_version,
            branch=branch,
            sha1=commit or 'latest',
            arch=get_arch()
        )
        try:
            shaman_response = urlopen(shaman_url)
        except HTTPError as err:
            logger.error('repository not found in shaman (might not be available yet)')
            raise Error('%s, failed to fetch %s' % (err, shaman_url))
        chacra_url = ''
        try:
            chacra_url = shaman_response.geturl()
            chacra_response = urlopen(chacra_url)
        except HTTPError as err:
            logger.error('repository not found in chacra (might not be available yet)')
            raise Error('%s, failed to fetch %s' % (err, chacra_url))
        return chacra_response.read().decode('utf-8')

    def repo_gpgkey(self) -> Tuple[str, str]:
        if self.ctx.gpg_url:
            return self.ctx.gpg_url, 'manual'
        if self.stable or self.version:
            return 'https://download.ceph.com/keys/release.gpg', 'release'
        else:
            return 'https://download.ceph.com/keys/autobuild.gpg', 'autobuild'

    def enable_service(self, service: str) -> None:
        """
        Start and enable the service (typically using systemd).
        """
        call_throws(self.ctx, ['systemctl', 'enable', '--now', service])


class Apt(Packager):
    DISTRO_NAMES = {
        'ubuntu': 'ubuntu',
        'debian': 'debian',
    }

    def __init__(self, ctx: CephadmContext,
                 stable: Optional[str], version: Optional[str], branch: Optional[str], commit: Optional[str],
                 distro: Optional[str], distro_version: Optional[str], distro_codename: Optional[str]) -> None:
        super(Apt, self).__init__(ctx, stable=stable, version=version,
                                  branch=branch, commit=commit)
        assert distro
        self.ctx = ctx
        self.distro = self.DISTRO_NAMES[distro]
        self.distro_codename = distro_codename
        self.distro_version = distro_version

    def repo_path(self) -> str:
        return '/etc/apt/sources.list.d/ceph.list'

    def add_repo(self) -> None:

        url, name = self.repo_gpgkey()
        logger.info('Installing repo GPG key from %s...' % url)
        try:
            response = urlopen(url)
        except HTTPError as err:
            logger.error('failed to fetch GPG repo key from %s: %s' % (
                url, err))
            raise Error('failed to fetch GPG key')
        key = response.read()
        with open('/etc/apt/trusted.gpg.d/ceph.%s.gpg' % name, 'wb') as f:
            f.write(key)

        if self.version:
            content = 'deb %s/debian-%s/ %s main\n' % (
                self.ctx.repo_url, self.version, self.distro_codename)
        elif self.stable:
            content = 'deb %s/debian-%s/ %s main\n' % (
                self.ctx.repo_url, self.stable, self.distro_codename)
        else:
            content = self.query_shaman(self.distro, self.distro_codename, self.branch,
                                        self.commit)

        logger.info('Installing repo file at %s...' % self.repo_path())
        with open(self.repo_path(), 'w') as f:
            f.write(content)

        self.update()

    def rm_repo(self) -> None:
        for name in ['autobuild', 'release', 'manual']:
            p = '/etc/apt/trusted.gpg.d/ceph.%s.gpg' % name
            if os.path.exists(p):
                logger.info('Removing repo GPG key %s...' % p)
                os.unlink(p)
        if os.path.exists(self.repo_path()):
            logger.info('Removing repo at %s...' % self.repo_path())
            os.unlink(self.repo_path())

        if self.distro == 'ubuntu':
            self.rm_kubic_repo()

    def install(self, ls: List[str]) -> None:
        logger.info('Installing packages %s...' % ls)
        call_throws(self.ctx, ['apt-get', 'install', '-y'] + ls)

    def update(self) -> None:
        logger.info('Updating package list...')
        call_throws(self.ctx, ['apt-get', 'update'])

    def install_podman(self) -> None:
        if self.distro == 'ubuntu':
            logger.info('Setting up repo for podman...')
            self.add_kubic_repo()
            self.update()

        logger.info('Attempting podman install...')
        try:
            self.install(['podman'])
        except Error:
            logger.info('Podman did not work.  Falling back to docker...')
            self.install(['docker.io'])

    def kubic_repo_url(self) -> str:
        return 'https://download.opensuse.org/repositories/devel:/kubic:/' \
               'libcontainers:/stable/xUbuntu_%s/' % self.distro_version

    def kubic_repo_path(self) -> str:
        return '/etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list'

    def kubic_repo_gpgkey_url(self) -> str:
        return '%s/Release.key' % self.kubic_repo_url()

    def kubic_repo_gpgkey_path(self) -> str:
        return '/etc/apt/trusted.gpg.d/kubic.release.gpg'

    def add_kubic_repo(self) -> None:
        url = self.kubic_repo_gpgkey_url()
        logger.info('Installing repo GPG key from %s...' % url)
        try:
            response = urlopen(url)
        except HTTPError as err:
            logger.error('failed to fetch GPG repo key from %s: %s' % (
                url, err))
            raise Error('failed to fetch GPG key')
        key = response.read().decode('utf-8')
        tmp_key = write_tmp(key, 0, 0)
        keyring = self.kubic_repo_gpgkey_path()
        call_throws(self.ctx, ['apt-key', '--keyring', keyring, 'add', tmp_key.name])

        logger.info('Installing repo file at %s...' % self.kubic_repo_path())
        content = 'deb %s /\n' % self.kubic_repo_url()
        with open(self.kubic_repo_path(), 'w') as f:
            f.write(content)

    def rm_kubic_repo(self) -> None:
        keyring = self.kubic_repo_gpgkey_path()
        if os.path.exists(keyring):
            logger.info('Removing repo GPG key %s...' % keyring)
            os.unlink(keyring)

        p = self.kubic_repo_path()
        if os.path.exists(p):
            logger.info('Removing repo at %s...' % p)
            os.unlink(p)


class YumDnf(Packager):
    DISTRO_NAMES = {
        'centos': ('centos', 'el'),
        'rhel': ('centos', 'el'),
        'scientific': ('centos', 'el'),
        'rocky': ('centos', 'el'),
        'almalinux': ('centos', 'el'),
        'ol': ('centos', 'el'),
        'fedora': ('fedora', 'fc'),
        'mariner': ('mariner', 'cm'),
    }

    def __init__(self, ctx: CephadmContext,
                 stable: Optional[str], version: Optional[str], branch: Optional[str], commit: Optional[str],
                 distro: Optional[str], distro_version: Optional[str]) -> None:
        super(YumDnf, self).__init__(ctx, stable=stable, version=version,
                                     branch=branch, commit=commit)
        assert distro
        assert distro_version
        self.ctx = ctx
        self.major = int(distro_version.split('.')[0])
        self.distro_normalized = self.DISTRO_NAMES[distro][0]
        self.distro_code = self.DISTRO_NAMES[distro][1] + str(self.major)
        if (self.distro_code == 'fc' and self.major >= 30) or \
           (self.distro_code == 'el' and self.major >= 8):
            self.tool = 'dnf'
        elif (self.distro_code == 'cm'):
            self.tool = 'tdnf'
        else:
            self.tool = 'yum'

    def custom_repo(self, **kw: Any) -> str:
        """
        Repo files need special care in that a whole line should not be present
        if there is no value for it. Because we were using `format()` we could
        not conditionally add a line for a repo file. So the end result would
        contain a key with a missing value (say if we were passing `None`).

        For example, it could look like::

        [ceph repo]
        name= ceph repo
        proxy=
        gpgcheck=

        Which breaks. This function allows us to conditionally add lines,
        preserving an order and be more careful.

        Previously, and for historical purposes, this is how the template used
        to look::

        custom_repo =
        [{repo_name}]
        name={name}
        baseurl={baseurl}
        enabled={enabled}
        gpgcheck={gpgcheck}
        type={_type}
        gpgkey={gpgkey}
        proxy={proxy}

        """
        lines = []

        # by using tuples (vs a dict) we preserve the order of what we want to
        # return, like starting with a [repo name]
        tmpl = (
            ('reponame', '[%s]'),
            ('name', 'name=%s'),
            ('baseurl', 'baseurl=%s'),
            ('enabled', 'enabled=%s'),
            ('gpgcheck', 'gpgcheck=%s'),
            ('_type', 'type=%s'),
            ('gpgkey', 'gpgkey=%s'),
            ('proxy', 'proxy=%s'),
            ('priority', 'priority=%s'),
        )

        for line in tmpl:
            tmpl_key, tmpl_value = line  # key values from tmpl

            # ensure that there is an actual value (not None nor empty string)
            if tmpl_key in kw and kw.get(tmpl_key) not in (None, ''):
                lines.append(tmpl_value % kw.get(tmpl_key))

        return '\n'.join(lines)

    def repo_path(self) -> str:
        return '/etc/yum.repos.d/ceph.repo'

    def repo_baseurl(self) -> str:
        assert self.stable or self.version
        if self.version:
            return '%s/rpm-%s/%s' % (self.ctx.repo_url, self.version,
                                     self.distro_code)
        else:
            return '%s/rpm-%s/%s' % (self.ctx.repo_url, self.stable,
                                     self.distro_code)

    def validate(self) -> None:
        if self.distro_code.startswith('fc'):
            raise Error('Ceph team does not build Fedora specific packages and therefore cannot add repos for this distro')
        if self.distro_code == 'el7':
            if self.stable and self.stable >= 'pacific':
                raise Error('Ceph does not support pacific or later for this version of this linux distro and therefore cannot add a repo for it')
            if self.version and self.version.split('.')[0] >= '16':
                raise Error('Ceph does not support 16.y.z or later for this version of this linux distro and therefore cannot add a repo for it')

        if self.stable or self.version:
            # we know that yum & dnf require there to be a
            # $base_url/$arch/repodata/repomd.xml so we can test if this URL
            # is gettable in order to validate the inputs
            test_url = self.repo_baseurl() + '/noarch/repodata/repomd.xml'
            try:
                urlopen(test_url)
            except HTTPError as err:
                logger.error('unable to fetch repo metadata: %r', err)
                raise Error('failed to fetch repository metadata. please check'
                            ' the provided parameters are correct and try again')

    def add_repo(self) -> None:
        if self.stable or self.version:
            content = ''
            for n, t in {
                    'Ceph': '$basearch',
                    'Ceph-noarch': 'noarch',
                    'Ceph-source': 'SRPMS'}.items():
                content += '[%s]\n' % (n)
                content += self.custom_repo(
                    name='Ceph %s' % t,
                    baseurl=self.repo_baseurl() + '/' + t,
                    enabled=1,
                    gpgcheck=1,
                    gpgkey=self.repo_gpgkey()[0],
                )
                content += '\n\n'
        else:
            content = self.query_shaman(self.distro_normalized, self.major,
                                        self.branch,
                                        self.commit)

        logger.info('Writing repo to %s...' % self.repo_path())
        with open(self.repo_path(), 'w') as f:
            f.write(content)

        if self.distro_code.startswith('el'):
            logger.info('Enabling EPEL...')
            call_throws(self.ctx, [self.tool, 'install', '-y', 'epel-release'])

    def rm_repo(self) -> None:
        if os.path.exists(self.repo_path()):
            os.unlink(self.repo_path())

    def install(self, ls: List[str]) -> None:
        logger.info('Installing packages %s...' % ls)
        call_throws(self.ctx, [self.tool, 'install', '-y'] + ls)

    def install_podman(self) -> None:
        self.install(['podman'])


class Zypper(Packager):
    DISTRO_NAMES = [
        'sles',
        'opensuse-tumbleweed',
        'opensuse-leap'
    ]

    def __init__(self, ctx: CephadmContext,
                 stable: Optional[str], version: Optional[str], branch: Optional[str], commit: Optional[str],
                 distro: Optional[str], distro_version: Optional[str]) -> None:
        super(Zypper, self).__init__(ctx, stable=stable, version=version,
                                     branch=branch, commit=commit)
        assert distro is not None
        self.ctx = ctx
        self.tool = 'zypper'
        self.distro = 'opensuse'
        self.distro_version = '15.1'
        if 'tumbleweed' not in distro and distro_version is not None:
            self.distro_version = distro_version

    def custom_repo(self, **kw: Any) -> str:
        """
        See YumDnf for format explanation.
        """
        lines = []

        # by using tuples (vs a dict) we preserve the order of what we want to
        # return, like starting with a [repo name]
        tmpl = (
            ('reponame', '[%s]'),
            ('name', 'name=%s'),
            ('baseurl', 'baseurl=%s'),
            ('enabled', 'enabled=%s'),
            ('gpgcheck', 'gpgcheck=%s'),
            ('_type', 'type=%s'),
            ('gpgkey', 'gpgkey=%s'),
            ('proxy', 'proxy=%s'),
            ('priority', 'priority=%s'),
        )

        for line in tmpl:
            tmpl_key, tmpl_value = line  # key values from tmpl

            # ensure that there is an actual value (not None nor empty string)
            if tmpl_key in kw and kw.get(tmpl_key) not in (None, ''):
                lines.append(tmpl_value % kw.get(tmpl_key))

        return '\n'.join(lines)

    def repo_path(self) -> str:
        return '/etc/zypp/repos.d/ceph.repo'

    def repo_baseurl(self) -> str:
        assert self.stable or self.version
        if self.version:
            return '%s/rpm-%s/%s' % (self.ctx.repo_url,
                                     self.stable, self.distro)
        else:
            return '%s/rpm-%s/%s' % (self.ctx.repo_url,
                                     self.stable, self.distro)

    def add_repo(self) -> None:
        if self.stable or self.version:
            content = ''
            for n, t in {
                    'Ceph': '$basearch',
                    'Ceph-noarch': 'noarch',
                    'Ceph-source': 'SRPMS'}.items():
                content += '[%s]\n' % (n)
                content += self.custom_repo(
                    name='Ceph %s' % t,
                    baseurl=self.repo_baseurl() + '/' + t,
                    enabled=1,
                    gpgcheck=1,
                    gpgkey=self.repo_gpgkey()[0],
                )
                content += '\n\n'
        else:
            content = self.query_shaman(self.distro, self.distro_version,
                                        self.branch,
                                        self.commit)

        logger.info('Writing repo to %s...' % self.repo_path())
        with open(self.repo_path(), 'w') as f:
            f.write(content)

    def rm_repo(self) -> None:
        if os.path.exists(self.repo_path()):
            os.unlink(self.repo_path())

    def install(self, ls: List[str]) -> None:
        logger.info('Installing packages %s...' % ls)
        call_throws(self.ctx, [self.tool, 'in', '-y'] + ls)

    def install_podman(self) -> None:
        self.install(['podman'])


def create_packager(ctx: CephadmContext,
                    stable: Optional[str] = None, version: Optional[str] = None,
                    branch: Optional[str] = None, commit: Optional[str] = None) -> Packager:
    distro, distro_version, distro_codename = get_distro()
    if distro in YumDnf.DISTRO_NAMES:
        return YumDnf(ctx, stable=stable, version=version,
                      branch=branch, commit=commit,
                      distro=distro, distro_version=distro_version)
    elif distro in Apt.DISTRO_NAMES:
        return Apt(ctx, stable=stable, version=version,
                   branch=branch, commit=commit,
                   distro=distro, distro_version=distro_version,
                   distro_codename=distro_codename)
    elif distro in Zypper.DISTRO_NAMES:
        return Zypper(ctx, stable=stable, version=version,
                      branch=branch, commit=commit,
                      distro=distro, distro_version=distro_version)
    raise Error('Distro %s version %s not supported' % (distro, distro_version))


def command_add_repo(ctx: CephadmContext) -> None:
    if ctx.version and ctx.release:
        raise Error('you can specify either --release or --version but not both')
    if not ctx.version and not ctx.release and not ctx.dev and not ctx.dev_commit:
        raise Error('please supply a --release, --version, --dev or --dev-commit argument')
    if ctx.version:
        try:
            (x, y, z) = ctx.version.split('.')
        except Exception:
            raise Error('version must be in the form x.y.z (e.g., 15.2.0)')
    if ctx.release:
        # Pacific =/= pacific in this case, set to undercase to avoid confusion
        ctx.release = ctx.release.lower()

    pkg = create_packager(ctx, stable=ctx.release,
                          version=ctx.version,
                          branch=ctx.dev,
                          commit=ctx.dev_commit)
    pkg.validate()
    pkg.add_repo()
    logger.info('Completed adding repo.')


def command_rm_repo(ctx: CephadmContext) -> None:
    pkg = create_packager(ctx)
    pkg.rm_repo()


def command_install(ctx: CephadmContext) -> None:
    pkg = create_packager(ctx)
    pkg.install(ctx.packages)


def command_rescan_disks(ctx: CephadmContext) -> str:

    def probe_hba(scan_path: str) -> None:
        """Tell the adapter to rescan"""
        with open(scan_path, 'w') as f:
            f.write('- - -')

    cmd = ctx.func.__name__.replace('command_', '')
    logger.info(f'{cmd}: starting')
    start = time.time()

    all_scan_files = glob('/sys/class/scsi_host/*/scan')
    scan_files = []
    skipped = []
    for scan_path in all_scan_files:
        adapter_name = os.path.basename(os.path.dirname(scan_path))
        proc_name = read_file([os.path.join(os.path.dirname(scan_path), 'proc_name')])
        if proc_name in ['unknown', 'usb-storage']:
            skipped.append(os.path.basename(scan_path))
            logger.info(f'{cmd}: rescan skipping incompatible host adapter {adapter_name} : {proc_name}')
            continue

        scan_files.append(scan_path)

    if not scan_files:
        logger.info(f'{cmd}: no compatible HBAs found')
        return 'Ok. No compatible HBAs found'

    responses = async_run(concurrent_tasks(probe_hba, scan_files))
    failures = [r for r in responses if r]

    logger.info(f'{cmd}: Complete. {len(scan_files)} adapters rescanned, {len(failures)} failures, {len(skipped)} skipped')

    elapsed = time.time() - start
    if failures:
        plural = 's' if len(failures) > 1 else ''
        if len(failures) == len(scan_files):
            return f'Failed. All {len(scan_files)} rescan requests failed'
        else:
            return f'Partial. {len(scan_files) - len(failures)} successful, {len(failures)} failure{plural} against: {", ".join(failures)}'

    return f'Ok. {len(all_scan_files)} adapters detected: {len(scan_files)} rescanned, {len(skipped)} skipped, {len(failures)} failed ({elapsed:.2f}s)'

##################################


def get_ipv4_address(ifname):
    # type: (str) -> str
    def _extract(sock: socket.socket, offset: int) -> str:
        return socket.inet_ntop(
            socket.AF_INET,
            fcntl.ioctl(
                sock.fileno(),
                offset,
                struct.pack('256s', bytes(ifname[:15], 'utf-8'))
            )[20:24])

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        addr = _extract(s, 35093)  # '0x8915' = SIOCGIFADDR
        dq_mask = _extract(s, 35099)  # 0x891b = SIOCGIFNETMASK
    except OSError:
        # interface does not have an ipv4 address
        return ''

    dec_mask = sum([bin(int(i)).count('1')
                    for i in dq_mask.split('.')])
    return '{}/{}'.format(addr, dec_mask)


def get_ipv6_address(ifname):
    # type: (str) -> str
    if not os.path.exists('/proc/net/if_inet6'):
        return ''

    raw = read_file(['/proc/net/if_inet6'])
    data = raw.splitlines()
    # based on docs @ https://www.tldp.org/HOWTO/Linux+IPv6-HOWTO/ch11s04.html
    # field 0 is ipv6, field 2 is scope
    for iface_setting in data:
        field = iface_setting.split()
        if field[-1] == ifname:
            ipv6_raw = field[0]
            ipv6_fmtd = ':'.join([ipv6_raw[_p:_p + 4] for _p in range(0, len(field[0]), 4)])
            # apply naming rules using ipaddress module
            ipv6 = ipaddress.ip_address(ipv6_fmtd)
            return '{}/{}'.format(str(ipv6), int('0x{}'.format(field[2]), 16))
    return ''


def bytes_to_human(num, mode='decimal'):
    # type: (float, str) -> str
    """Convert a bytes value into it's human-readable form.

    :param num: number, in bytes, to convert
    :param mode: Either decimal (default) or binary to determine divisor
    :returns: string representing the bytes value in a more readable format
    """
    unit_list = ['', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB']
    divisor = 1000.0
    yotta = 'YB'

    if mode == 'binary':
        unit_list = ['', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB']
        divisor = 1024.0
        yotta = 'YiB'

    for unit in unit_list:
        if abs(num) < divisor:
            return '%3.1f%s' % (num, unit)
        num /= divisor
    return '%.1f%s' % (num, yotta)


def read_file(path_list, file_name=''):
    # type: (List[str], str) -> str
    """Returns the content of the first file found within the `path_list`

    :param path_list: list of file paths to search
    :param file_name: optional file_name to be applied to a file path
    :returns: content of the file or 'Unknown'
    """
    for path in path_list:
        if file_name:
            file_path = os.path.join(path, file_name)
        else:
            file_path = path
        if os.path.exists(file_path):
            with open(file_path, 'rb') as f:
                try:
                    content = f.read().decode('utf-8', 'ignore').strip()
                except OSError:
                    # sysfs may populate the file, but for devices like
                    # virtio reads can fail
                    return 'Unknown'
                else:
                    return content
    return 'Unknown'

##################################


class Enclosure:
    def __init__(self, enc_id: str, enc_path: str, dev_path: str):
        """External disk enclosure metadata

        Args:
        :param enc_id: enclosure id (normally a WWN)
        :param enc_path: sysfs path to HBA attached to the enclosure
                         e.g. /sys/class/scsi_generic/sg11/device/enclosure/0:0:9:0
        :param dev_path: sysfs path to the generic scsi device for the enclosure HBA
                         e.g. /sys/class/scsi_generic/sg2
        """
        self._path: str = dev_path
        self._dev_path: str = os.path.join(dev_path, 'device')
        self._enc_path: str = enc_path
        self.ses_paths: List[str] = []
        self.path_count: int = 0
        self.vendor: str = ''
        self.model: str = ''
        self.enc_id: str = enc_id
        self.components: Union[int, str] = 0
        self.device_lookup: Dict[str, str] = {}
        self.device_count: int = 0
        self.slot_map: Dict[str, Dict[str, str]] = {}

        self._probe()

    def _probe(self) -> None:
        """Analyse the dev paths to identify enclosure related information"""

        self.vendor = read_file([os.path.join(self._dev_path, 'vendor')])
        self.model = read_file([os.path.join(self._dev_path, 'model')])
        self.components = read_file([os.path.join(self._enc_path, 'components')])
        slot_paths = glob(os.path.join(self._enc_path, '*', 'slot'))
        for slot_path in slot_paths:
            slot = read_file([slot_path])
            serial_path = os.path.join(os.path.dirname(slot_path), 'device', 'vpd_pg80')
            serial = ''
            if os.path.exists(serial_path):
                serial_raw = read_file([serial_path])
                serial = (''.join(char for char in serial_raw if char in string.printable)).strip()
                self.device_lookup[serial] = slot
            slot_dir = os.path.dirname(slot_path)
            self.slot_map[slot] = {
                'status': read_file([os.path.join(slot_dir, 'status')]),
                'fault': read_file([os.path.join(slot_dir, 'fault')]),
                'locate': read_file([os.path.join(slot_dir, 'locate')]),
                'serial': serial,
            }

        self.device_count = len(self.device_lookup)
        self.update(os.path.basename(self._path))

    def update(self, dev_id: str) -> None:
        """Update an enclosure object with a related sg device name

        :param dev_id (str): device name e.g. sg2
        """
        self.ses_paths.append(dev_id)
        self.path_count = len(self.ses_paths)

    def _dump(self) -> Dict[str, Any]:
        """Return a dict representation of the object"""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}

    def __str__(self) -> str:
        """Return a formatted json representation of the object as a string"""
        return json.dumps(self._dump(), indent=2)

    def __repr__(self) -> str:
        """Return a json representation of the object as a string"""
        return json.dumps(self._dump())

    def as_json(self) -> Dict[str, Any]:
        """Return a dict representing the object"""
        return self._dump()


class HostFacts():
    _dmi_path_list = ['/sys/class/dmi/id']
    _nic_path_list = ['/sys/class/net']
    _apparmor_path_list = ['/etc/apparmor']
    _disk_vendor_workarounds = {
        '0x1af4': 'Virtio Block Device'
    }
    _excluded_block_devices = ('sr', 'zram', 'dm-', 'loop', 'md')
    _sg_generic_glob = '/sys/class/scsi_generic/*'

    def __init__(self, ctx: CephadmContext):
        self.ctx: CephadmContext = ctx
        self.cpu_model: str = 'Unknown'
        self.sysctl_options: Dict[str, str] = self._populate_sysctl_options()
        self.cpu_count: int = 0
        self.cpu_cores: int = 0
        self.cpu_threads: int = 0
        self.interfaces: Dict[str, Any] = {}

        self._meminfo: List[str] = read_file(['/proc/meminfo']).splitlines()
        self._get_cpuinfo()
        self._process_nics()
        self.arch: str = platform.processor()
        self.kernel: str = platform.release()
        self._enclosures = self._discover_enclosures()
        self._block_devices = self._get_block_devs()
        self._device_list = self._get_device_info()

    def _populate_sysctl_options(self) -> Dict[str, str]:
        sysctl_options = {}
        out, _, _ = call_throws(self.ctx, ['sysctl', '-a'], verbosity=CallVerbosity.QUIET_UNLESS_ERROR)
        if out:
            for line in out.splitlines():
                option, value = line.split('=')
                sysctl_options[option.strip()] = value.strip()
        return sysctl_options

    def _discover_enclosures(self) -> Dict[str, Enclosure]:
        """Build a dictionary of discovered scsi enclosures

        Enclosures are detected by walking the scsi generic sysfs hierarchy.
        Any device tree that holds an 'enclosure' subdirectory is interpreted as
        an enclosure. Once identified the enclosure directory is analysis to
        identify key descriptors that will help relate disks to enclosures and
        disks to enclosure slots.

        :return: Dict[str, Enclosure]: a map of enclosure id (hex) to enclosure object
        """
        sg_paths: List[str] = glob(HostFacts._sg_generic_glob)
        enclosures: Dict[str, Enclosure] = {}

        for sg_path in sg_paths:
            enc_path = os.path.join(sg_path, 'device', 'enclosure')
            if os.path.exists(enc_path):
                enc_dirs = glob(os.path.join(enc_path, '*'))
                if len(enc_dirs) != 1:
                    # incomplete enclosure spec - expecting ONE dir in the format
                    # host(adapter):bus:target:lun e.g. 16:0:0:0
                    continue
                enc_path = enc_dirs[0]
                enc_id = read_file([os.path.join(enc_path, 'id')])
                if enc_id in enclosures:
                    enclosures[enc_id].update(os.path.basename(sg_path))
                    continue

                enclosure = Enclosure(enc_id, enc_path, sg_path)
                enclosures[enc_id] = enclosure

        return enclosures

    @property
    def enclosures(self) -> Dict[str, Dict[str, Any]]:
        """Dump the enclosure objects as dicts"""
        return {k: v._dump() for k, v in self._enclosures.items()}

    @property
    def enclosure_count(self) -> int:
        """Return the number of enclosures detected"""
        return len(self._enclosures.keys())

    def _get_cpuinfo(self):
        # type: () -> None
        """Determine cpu information via /proc/cpuinfo"""
        raw = read_file(['/proc/cpuinfo'])
        output = raw.splitlines()
        cpu_set = set()

        for line in output:
            field = [f.strip() for f in line.split(':')]
            if 'model name' in line:
                self.cpu_model = field[1]
            if 'physical id' in line:
                cpu_set.add(field[1])
            if 'siblings' in line:
                self.cpu_threads = int(field[1].strip())
            if 'cpu cores' in line:
                self.cpu_cores = int(field[1].strip())
            pass
        self.cpu_count = len(cpu_set)

    def _get_block_devs(self):
        # type: () -> List[str]
        """Determine the list of block devices by looking at /sys/block"""
        return [dev for dev in os.listdir('/sys/block')
                if not dev.startswith(HostFacts._excluded_block_devices)]

    @property
    def operating_system(self):
        # type: () -> str
        """Determine OS version"""
        raw_info = read_file(['/etc/os-release'])
        os_release = raw_info.splitlines()
        rel_str = 'Unknown'
        rel_dict = dict()

        for line in os_release:
            if '=' in line:
                var_name, var_value = line.split('=')
                rel_dict[var_name] = var_value.strip('"')

        # Would normally use PRETTY_NAME, but NAME and VERSION are more
        # consistent
        if all(_v in rel_dict for _v in ['NAME', 'VERSION']):
            rel_str = '{} {}'.format(rel_dict['NAME'], rel_dict['VERSION'])
        return rel_str

    @property
    def hostname(self):
        # type: () -> str
        """Return the hostname"""
        return platform.node()

    @property
    def shortname(self) -> str:
        return platform.node().split('.', 1)[0]

    @property
    def fqdn(self) -> str:
        return get_fqdn()

    @property
    def subscribed(self):
        # type: () -> str
        """Highlevel check to see if the host is subscribed to receive updates/support"""
        def _red_hat():
            # type: () -> str
            # RHEL 7 and RHEL 8
            entitlements_dir = '/etc/pki/entitlement'
            if os.path.exists(entitlements_dir):
                pems = glob('{}/*.pem'.format(entitlements_dir))
                if len(pems) >= 2:
                    return 'Yes'

            return 'No'

        os_name = self.operating_system
        if os_name.upper().startswith('RED HAT'):
            return _red_hat()

        return 'Unknown'

    @property
    def hdd_count(self):
        # type: () -> int
        """Return a count of HDDs (spinners)"""
        return len(self.hdd_list)

    def _get_capacity(self, dev):
        # type: (str) -> int
        """Determine the size of a given device

        The kernel always bases device size calculations based on a 512 byte
        sector. For more information see
        https://git.kernel.org/pub/scm/linux/kernel/git/stable/linux.git/tree/include/linux/types.h?h=v5.15.63#n120
        """
        size_path = os.path.join('/sys/block', dev, 'size')
        size_blocks = int(read_file([size_path]))
        return size_blocks * 512

    def _get_capacity_by_type(self, disk_type='hdd'):
        # type: (str) -> int
        """Return the total capacity of a category of device (flash or hdd)"""
        capacity: int = 0
        for dev in self._device_list:
            if dev['disk_type'] == disk_type:
                disk_capacity = cast(int, dev.get('disk_size_bytes', 0))
                capacity += disk_capacity
        return capacity

    def _get_device_info(self):
        # type: () -> List[Dict[str, object]]
        """Return a 'pretty' name list for each unique device in the `dev_list`"""
        disk_list = list()

        # serial_num_lookup is a dict of serial number -> List of devices with that serial number
        serial_num_lookup: Dict[str, List[str]] = {}

        # make a map of devname -> disk path. this path name may indicate the physical slot
        # of a drive (phyXX)
        disk_path_map: Dict[str, str] = {}
        for path in glob('/dev/disk/by-path/*'):
            tgt_raw = Path(path).resolve()
            tgt = os.path.basename(str(tgt_raw))
            disk_path_map[tgt] = path

        # make a map of holder (dm-XX) -> full mpath name
        dm_device_map: Dict[str, str] = {}
        for mpath in glob('/dev/mapper/mpath*'):
            tgt_raw = Path(mpath).resolve()
            tgt = os.path.basename(str(tgt_raw))
            dm_device_map[tgt] = mpath

        # main loop to process all eligible block devices
        for dev in self._block_devices:
            enclosure_id = ''
            enclosure_slot = ''
            scsi_addr = ''
            mpath = ''

            disk_model = read_file(['/sys/block/{}/device/model'.format(dev)]).strip()
            disk_rev = read_file(['/sys/block/{}/device/rev'.format(dev)]).strip()
            disk_wwid = read_file(['/sys/block/{}/device/wwid'.format(dev)]).strip()
            vendor = read_file(['/sys/block/{}/device/vendor'.format(dev)]).strip()
            rotational = read_file(['/sys/block/{}/queue/rotational'.format(dev)])
            holders_raw = glob('/sys/block/{}/holders/*'.format(dev))
            if len(holders_raw) == 1:
                # mpath will have 1 holder entry
                holder = os.path.basename(holders_raw[0])
                mpath = dm_device_map.get(holder, '')

            disk_type = 'hdd' if rotational == '1' else 'flash'
            scsi_addr_path = glob('/sys/block/{}/device/bsg/*'.format(dev))
            if len(scsi_addr_path) == 1:
                scsi_addr = os.path.basename(scsi_addr_path[0])

            # vpd_pg80 isn't guaranteed (libvirt, vmware for example)
            serial_raw = read_file(['/sys/block/{}/device/vpd_pg80'.format(dev)])
            serial = (''.join(i for i in serial_raw if i in string.printable)).strip()
            if serial.lower() == 'unknown':
                serial = ''
            else:
                if serial in serial_num_lookup:
                    serial_num_lookup[serial].append(dev)
                else:
                    serial_num_lookup[serial] = [dev]
                for enc_id, enclosure in self._enclosures.items():
                    if serial in enclosure.device_lookup.keys():
                        enclosure_id = enc_id
                        enclosure_slot = enclosure.device_lookup[serial]

            disk_vendor = HostFacts._disk_vendor_workarounds.get(vendor, vendor)
            disk_size_bytes = self._get_capacity(dev)
            disk_list.append({
                'description': '{} {} ({})'.format(disk_vendor, disk_model, bytes_to_human(disk_size_bytes)),
                'vendor': disk_vendor,
                'model': disk_model,
                'rev': disk_rev,
                'wwid': disk_wwid,
                'dev_name': dev,
                'disk_size_bytes': disk_size_bytes,
                'disk_type': disk_type,
                'serial': serial,
                'alt_dev_name': '',
                'scsi_addr': scsi_addr,
                'enclosure_id': enclosure_id,
                'enclosure_slot': enclosure_slot,
                'path_id': disk_path_map.get(dev, ''),
                'mpath': mpath,
            })

        # process the devices to drop duplicate physical devs based on matching
        # the unique serial number
        disk_list_unique: List[Dict[str, Any]] = []
        serials_seen: List[str] = []
        for dev in disk_list:
            serial = str(dev['serial'])
            if serial:
                if serial in serials_seen:
                    continue
                else:
                    serials_seen.append(serial)
                    devs = serial_num_lookup[serial].copy()
                    devs.remove(str(dev['dev_name']))
                    dev['alt_dev_name'] = ','.join(devs)
            disk_list_unique.append(dev)

        return disk_list_unique

    @property
    def hdd_list(self):
        # type: () -> List[Dict[str, object]]
        """Return a list of devices that are HDDs (spinners)"""
        return [dev for dev in self._device_list if dev['disk_type'] == 'hdd']

    @property
    def flash_list(self):
        # type: () -> List[Dict[str, object]]
        """Return a list of devices that are flash based (SSD, NVMe)"""
        return [dev for dev in self._device_list if dev['disk_type'] == 'flash']

    @property
    def hdd_capacity_bytes(self):
        # type: () -> int
        """Return the total capacity for all HDD devices (bytes)"""
        return self._get_capacity_by_type(disk_type='hdd')

    @property
    def hdd_capacity(self):
        # type: () -> str
        """Return the total capacity for all HDD devices (human readable format)"""
        return bytes_to_human(self.hdd_capacity_bytes)

    @property
    def cpu_load(self):
        # type: () -> Dict[str, float]
        """Return the cpu load average data for the host"""
        raw = read_file(['/proc/loadavg']).strip()
        data = raw.split()
        return {
            '1min': float(data[0]),
            '5min': float(data[1]),
            '15min': float(data[2]),
        }

    @property
    def flash_count(self):
        # type: () -> int
        """Return the number of flash devices in the system (SSD, NVMe)"""
        return len(self.flash_list)

    @property
    def flash_capacity_bytes(self):
        # type: () -> int
        """Return the total capacity for all flash devices (bytes)"""
        return self._get_capacity_by_type(disk_type='flash')

    @property
    def flash_capacity(self):
        # type: () -> str
        """Return the total capacity for all Flash devices (human readable format)"""
        return bytes_to_human(self.flash_capacity_bytes)

    def _process_nics(self):
        # type: () -> None
        """Look at the NIC devices and extract network related metadata"""
        # from https://github.com/torvalds/linux/blob/master/include/uapi/linux/if_arp.h
        hw_lookup = {
            '1': 'ethernet',
            '32': 'infiniband',
            '772': 'loopback',
        }

        for nic_path in HostFacts._nic_path_list:
            if not os.path.exists(nic_path):
                continue
            for iface in os.listdir(nic_path):

                if os.path.exists(os.path.join(nic_path, iface, 'bridge')):
                    nic_type = 'bridge'
                elif os.path.exists(os.path.join(nic_path, iface, 'bonding')):
                    nic_type = 'bonding'
                else:
                    nic_type = hw_lookup.get(read_file([os.path.join(nic_path, iface, 'type')]), 'Unknown')

                if nic_type == 'loopback':  # skip loopback devices
                    continue

                lower_devs_list = [os.path.basename(link.replace('lower_', '')) for link in glob(os.path.join(nic_path, iface, 'lower_*'))]
                upper_devs_list = [os.path.basename(link.replace('upper_', '')) for link in glob(os.path.join(nic_path, iface, 'upper_*'))]

                try:
                    mtu = int(read_file([os.path.join(nic_path, iface, 'mtu')]))
                except ValueError:
                    mtu = 0

                operstate = read_file([os.path.join(nic_path, iface, 'operstate')])
                try:
                    speed = int(read_file([os.path.join(nic_path, iface, 'speed')]))
                except (OSError, ValueError):
                    # OSError : device doesn't support the ethtool get_link_ksettings
                    # ValueError : raised when the read fails, and returns Unknown
                    #
                    # Either way, we show a -1 when speed isn't available
                    speed = -1

                dev_link = os.path.join(nic_path, iface, 'device')
                if os.path.exists(dev_link):
                    iftype = 'physical'
                    driver_path = os.path.join(dev_link, 'driver')
                    if os.path.exists(driver_path):
                        driver = os.path.basename(os.path.realpath(driver_path))
                    else:
                        driver = 'Unknown'

                else:
                    iftype = 'logical'
                    driver = ''

                self.interfaces[iface] = {
                    'mtu': mtu,
                    'upper_devs_list': upper_devs_list,
                    'lower_devs_list': lower_devs_list,
                    'operstate': operstate,
                    'iftype': iftype,
                    'nic_type': nic_type,
                    'driver': driver,
                    'speed': speed,
                    'ipv4_address': get_ipv4_address(iface),
                    'ipv6_address': get_ipv6_address(iface),
                }

    @property
    def nic_count(self):
        # type: () -> int
        """Return a total count of all physical NICs detected in the host"""
        phys_devs = []
        for iface in self.interfaces:
            if self.interfaces[iface]['iftype'] == 'physical':
                phys_devs.append(iface)
        return len(phys_devs)

    def _get_mem_data(self, field_name):
        # type: (str) -> int
        for line in self._meminfo:
            if line.startswith(field_name):
                _d = line.split()
                return int(_d[1])
        return 0

    @property
    def memory_total_kb(self):
        # type: () -> int
        """Determine the memory installed (kb)"""
        return self._get_mem_data('MemTotal')

    @property
    def memory_free_kb(self):
        # type: () -> int
        """Determine the memory free (not cache, immediately usable)"""
        return self._get_mem_data('MemFree')

    @property
    def memory_available_kb(self):
        # type: () -> int
        """Determine the memory available to new applications without swapping"""
        return self._get_mem_data('MemAvailable')

    @property
    def vendor(self):
        # type: () -> str
        """Determine server vendor from DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'sys_vendor')

    @property
    def model(self):
        # type: () -> str
        """Determine server model information from DMI data in sysfs"""
        family = read_file(HostFacts._dmi_path_list, 'product_family')
        product = read_file(HostFacts._dmi_path_list, 'product_name')
        if family == 'Unknown' and product:
            return '{}'.format(product)

        return '{} ({})'.format(family, product)

    @property
    def bios_version(self):
        # type: () -> str
        """Determine server BIOS version from  DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'bios_version')

    @property
    def bios_date(self):
        # type: () -> str
        """Determine server BIOS date from  DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'bios_date')

    @property
    def chassis_serial(self):
        # type: () -> str
        """Determine chassis serial number from DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'chassis_serial')

    @property
    def board_serial(self):
        # type: () -> str
        """Determine mainboard serial number from DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'board_serial')

    @property
    def product_serial(self):
        # type: () -> str
        """Determine server's serial number from DMI data in sysfs"""
        return read_file(HostFacts._dmi_path_list, 'product_serial')

    @property
    def timestamp(self):
        # type: () -> float
        """Return the current time as Epoch seconds"""
        return time.time()

    @property
    def system_uptime(self):
        # type: () -> float
        """Return the system uptime (in secs)"""
        raw_time = read_file(['/proc/uptime'])
        up_secs, _ = raw_time.split()
        return float(up_secs)

    @property
    def kernel_security(self):
        # type: () -> Dict[str, str]
        """Determine the security features enabled in the kernel - SELinux, AppArmor"""
        def _fetch_selinux() -> Dict[str, str]:
            """Get the selinux status"""
            security = {}
            try:
                out, err, code = call(self.ctx, ['sestatus'],
                                      verbosity=CallVerbosity.QUIET)
                security['type'] = 'SELinux'
                status, mode, policy = '', '', ''
                for line in out.split('\n'):
                    if line.startswith('SELinux status:'):
                        k, v = line.split(':')
                        status = v.strip()
                    elif line.startswith('Current mode:'):
                        k, v = line.split(':')
                        mode = v.strip()
                    elif line.startswith('Loaded policy name:'):
                        k, v = line.split(':')
                        policy = v.strip()
                if status == 'disabled':
                    security['description'] = 'SELinux: Disabled'
                else:
                    security['description'] = 'SELinux: Enabled({}, {})'.format(mode, policy)
            except Exception as e:
                logger.info('unable to get selinux status: %s' % e)
            return security

        def _fetch_apparmor() -> Dict[str, str]:
            """Read the apparmor profiles directly, returning an overview of AppArmor status"""
            security = {}
            for apparmor_path in HostFacts._apparmor_path_list:
                if os.path.exists(apparmor_path):
                    security['type'] = 'AppArmor'
                    security['description'] = 'AppArmor: Enabled'
                    try:
                        profiles = read_file(['/sys/kernel/security/apparmor/profiles'])
                        if len(profiles) == 0:
                            return {}
                    except OSError:
                        pass
                    else:
                        summary = {}  # type: Dict[str, int]
                        for line in profiles.split('\n'):
                            item, mode = line.split(' ')
                            mode = mode.strip('()')
                            if mode in summary:
                                summary[mode] += 1
                            else:
                                summary[mode] = 0
                        summary_str = ','.join(['{} {}'.format(v, k) for k, v in summary.items()])
                        security = {**security, **summary}  # type: ignore
                        security['description'] += '({})'.format(summary_str)

                    return security
            return {}

        ret = {}
        if os.path.exists('/sys/kernel/security/lsm'):
            lsm = read_file(['/sys/kernel/security/lsm']).strip()
            if 'selinux' in lsm:
                ret = _fetch_selinux()
            elif 'apparmor' in lsm:
                ret = _fetch_apparmor()
            else:
                return {
                    'type': 'Unknown',
                    'description': 'Linux Security Module framework is active, but is not using SELinux or AppArmor'
                }

        if ret:
            return ret

        return {
            'type': 'None',
            'description': 'Linux Security Module framework is not available'
        }

    @property
    def selinux_enabled(self) -> bool:
        return (self.kernel_security['type'] == 'SELinux') and \
               (self.kernel_security['description'] != 'SELinux: Disabled')

    @property
    def kernel_parameters(self):
        # type: () -> Dict[str, str]
        """Get kernel parameters required/used in Ceph clusters"""

        k_param = {}
        out, _, _ = call_throws(self.ctx, ['sysctl', '-a'], verbosity=CallVerbosity.SILENT)
        if out:
            param_list = out.split('\n')
            param_dict = {param.split(' = ')[0]: param.split(' = ')[-1] for param in param_list}

            # return only desired parameters
            if 'net.ipv4.ip_nonlocal_bind' in param_dict:
                k_param['net.ipv4.ip_nonlocal_bind'] = param_dict['net.ipv4.ip_nonlocal_bind']

        return k_param

    @staticmethod
    def _process_net_data(tcp_file: str, protocol: str = 'tcp') -> List[int]:
        listening_ports = []
        # Connections state documentation
        # tcp - https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/include/net/tcp_states.h
        # udp - uses 07 (TCP_CLOSE or UNCONN, since udp is stateless. test with netcat -ul <port>)
        listening_state = {
            'tcp': '0A',
            'udp': '07'
        }

        if protocol not in listening_state.keys():
            return []

        if os.path.exists(tcp_file):
            with open(tcp_file) as f:
                tcp_data = f.readlines()[1:]

            for con in tcp_data:
                con_info = con.strip().split()
                if con_info[3] == listening_state[protocol]:
                    local_port = int(con_info[1].split(':')[1], 16)
                    listening_ports.append(local_port)

        return listening_ports

    @property
    def tcp_ports_used(self) -> List[int]:
        return HostFacts._process_net_data('/proc/net/tcp')

    @property
    def tcp6_ports_used(self) -> List[int]:
        return HostFacts._process_net_data('/proc/net/tcp6')

    @property
    def udp_ports_used(self) -> List[int]:
        return HostFacts._process_net_data('/proc/net/udp', 'udp')

    @property
    def udp6_ports_used(self) -> List[int]:
        return HostFacts._process_net_data('/proc/net/udp6', 'udp')

    def dump(self):
        # type: () -> str
        """Return the attributes of this HostFacts object as json"""
        data = {
            k: getattr(self, k) for k in dir(self)
            if not k.startswith('_')
            and isinstance(getattr(self, k), (float, int, str, list, dict, tuple))
        }
        return json.dumps(data, indent=2, sort_keys=True)

##################################


def command_gather_facts(ctx: CephadmContext) -> None:
    """gather_facts is intended to provide host related metadata to the caller"""
    host = HostFacts(ctx)
    print(host.dump())


##################################


def systemd_target_state(ctx: CephadmContext, target_name: str, subsystem: str = 'ceph') -> bool:
    # TODO: UNITTEST
    return os.path.exists(
        os.path.join(
            ctx.unit_dir,
            f'{subsystem}.target.wants',
            target_name
        )
    )


def target_exists(ctx: CephadmContext) -> bool:
    return os.path.exists(ctx.unit_dir + '/ceph.target')


@infer_fsid
def command_maintenance(ctx: CephadmContext) -> str:
    if not ctx.fsid:
        raise Error('failed - must pass --fsid to specify cluster')

    target = f'ceph-{ctx.fsid}.target'

    if ctx.maintenance_action.lower() == 'enter':
        logger.info('Requested to place host into maintenance')
        if systemd_target_state(ctx, target):
            _out, _err, code = call(ctx,
                                    ['systemctl', 'disable', target],
                                    verbosity=CallVerbosity.DEBUG)
            if code:
                logger.error(f'Failed to disable the {target} target')
                return 'failed - to disable the target'
            else:
                # stopping a target waits by default
                _out, _err, code = call(ctx,
                                        ['systemctl', 'stop', target],
                                        verbosity=CallVerbosity.DEBUG)
                if code:
                    logger.error(f'Failed to stop the {target} target')
                    return 'failed - to disable the target'
                else:
                    return f'success - systemd target {target} disabled'

        else:
            return 'skipped - target already disabled'

    else:
        logger.info('Requested to exit maintenance state')
        # if we've never deployed a daemon on this host there will be no systemd
        # target to disable so attempting a disable will fail. We still need to
        # return success here or host will be permanently stuck in maintenance mode
        # as no daemons can be deployed so no systemd target will ever exist to disable.
        if not target_exists(ctx):
            return 'skipped - systemd target not present on this host. Host removed from maintenance mode.'
        # exit maintenance request
        if not systemd_target_state(ctx, target):
            _out, _err, code = call(ctx,
                                    ['systemctl', 'enable', target],
                                    verbosity=CallVerbosity.DEBUG)
            if code:
                logger.error(f'Failed to enable the {target} target')
                return 'failed - unable to enable the target'
            else:
                # starting a target waits by default
                _out, _err, code = call(ctx,
                                        ['systemctl', 'start', target],
                                        verbosity=CallVerbosity.DEBUG)
                if code:
                    logger.error(f'Failed to start the {target} target')
                    return 'failed - unable to start the target'
                else:
                    return f'success - systemd target {target} enabled and started'
        return f'success - systemd target {target} enabled and started'

##################################


class ArgumentFacade:
    def __init__(self) -> None:
        self.defaults: Dict[str, Any] = {}

    def add_argument(self, *args: Any, **kwargs: Any) -> None:
        if not args:
            raise ValueError('expected at least one argument')
        name = args[0]
        if not name.startswith('--'):
            raise ValueError(f'expected long option, got: {name!r}')
        name = name[2:].replace('-', '_')
        value = kwargs.pop('default', None)
        self.defaults[name] = value

    def apply(self, ctx: CephadmContext) -> None:
        for key, value in self.defaults.items():
            setattr(ctx, key, value)


def _add_deploy_parser_args(
    parser_deploy: Union[argparse.ArgumentParser, ArgumentFacade],
) -> None:
    parser_deploy.add_argument(
        '--config', '-c',
        help='config file for new daemon')
    parser_deploy.add_argument(
        '--config-json',
        help='Additional configuration information in JSON format')
    parser_deploy.add_argument(
        '--keyring',
        help='keyring for new daemon')
    parser_deploy.add_argument(
        '--key',
        help='key for new daemon')
    parser_deploy.add_argument(
        '--osd-fsid',
        help='OSD uuid, if creating an OSD container')
    parser_deploy.add_argument(
        '--skip-firewalld',
        action='store_true',
        help='Do not configure firewalld')
    parser_deploy.add_argument(
        '--tcp-ports',
        help='List of tcp ports to open in the host firewall')
    parser_deploy.add_argument(
        '--port-ips',
        help='JSON dict mapping ports to IPs they need to be bound on'
    )
    parser_deploy.add_argument(
        '--reconfig',
        action='store_true',
        help='Reconfigure a previously deployed daemon')
    parser_deploy.add_argument(
        '--allow-ptrace',
        action='store_true',
        help='Allow SYS_PTRACE on daemon container')
    parser_deploy.add_argument(
        '--container-init',
        action='store_true',
        default=CONTAINER_INIT,
        help=argparse.SUPPRESS)
    parser_deploy.add_argument(
        '--memory-request',
        help='Container memory request/target'
    )
    parser_deploy.add_argument(
        '--memory-limit',
        help='Container memory hard limit'
    )
    parser_deploy.add_argument(
        '--meta-json',
        help='JSON dict of additional metadata'
    )
    parser_deploy.add_argument(
        '--extra-container-args',
        action='append',
        default=[],
        help='Additional container arguments to apply to daemon'
    )
    parser_deploy.add_argument(
        '--extra-entrypoint-args',
        action='append',
        default=[],
        help='Additional entrypoint arguments to apply to deamon'
    )


def _get_parser():
    # type: () -> argparse.ArgumentParser
    parser = argparse.ArgumentParser(
        description='Bootstrap Ceph daemons with systemd and containers.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--image',
        help='container image. Can also be set via the "CEPHADM_IMAGE" '
        'env var')
    parser.add_argument(
        '--docker',
        action='store_true',
        help='use docker instead of podman')
    parser.add_argument(
        '--data-dir',
        default=DATA_DIR,
        help='base directory for daemon data')
    parser.add_argument(
        '--log-dir',
        default=LOG_DIR,
        help='base directory for daemon logs')
    parser.add_argument(
        '--logrotate-dir',
        default=LOGROTATE_DIR,
        help='location of logrotate configuration files')
    parser.add_argument(
        '--sysctl-dir',
        default=SYSCTL_DIR,
        help='location of sysctl configuration files')
    parser.add_argument(
        '--unit-dir',
        default=UNIT_DIR,
        help='base directory for systemd units')
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show debug-level log messages')
    parser.add_argument(
        '--timeout',
        type=int,
        default=DEFAULT_TIMEOUT,
        help='timeout in seconds')
    parser.add_argument(
        '--retry',
        type=int,
        default=DEFAULT_RETRY,
        help='max number of retries')
    parser.add_argument(
        '--env', '-e',
        action='append',
        default=[],
        help='set environment variable')
    parser.add_argument(
        '--no-container-init',
        action='store_true',
        default=not CONTAINER_INIT,
        help='Do not run podman/docker with `--init`')
    parser.add_argument(
        '--no-cgroups-split',
        action='store_true',
        default=False,
        help='Do not run containers with --cgroups=split (currently only relevant when using podman)')

    subparsers = parser.add_subparsers(help='sub-command')

    parser_version = subparsers.add_parser(
        'version', help='get cephadm version')
    parser_version.set_defaults(func=command_version)

    parser_pull = subparsers.add_parser(
        'pull', help='pull the default container image')
    parser_pull.set_defaults(func=command_pull)
    parser_pull.add_argument(
        '--insecure',
        action='store_true',
        help=argparse.SUPPRESS,
    )

    parser_inspect_image = subparsers.add_parser(
        'inspect-image', help='inspect local container image')
    parser_inspect_image.set_defaults(func=command_inspect_image)

    parser_ls = subparsers.add_parser(
        'ls', help='list daemon instances on this host')
    parser_ls.set_defaults(func=command_ls)
    parser_ls.add_argument(
        '--no-detail',
        action='store_true',
        help='Do not include daemon status')
    parser_ls.add_argument(
        '--legacy-dir',
        default='/',
        help='base directory for legacy daemon data')

    parser_list_networks = subparsers.add_parser(
        'list-networks', help='list IP networks')
    parser_list_networks.set_defaults(func=command_list_networks)

    parser_adopt = subparsers.add_parser(
        'adopt', help='adopt daemon deployed with a different tool')
    parser_adopt.set_defaults(func=command_adopt)
    parser_adopt.add_argument(
        '--name', '-n',
        required=True,
        help='daemon name (type.id)')
    parser_adopt.add_argument(
        '--style',
        required=True,
        help='deployment style (legacy, ...)')
    parser_adopt.add_argument(
        '--cluster',
        default='ceph',
        help='cluster name')
    parser_adopt.add_argument(
        '--legacy-dir',
        default='/',
        help='base directory for legacy daemon data')
    parser_adopt.add_argument(
        '--config-json',
        help='Additional configuration information in JSON format')
    parser_adopt.add_argument(
        '--skip-firewalld',
        action='store_true',
        help='Do not configure firewalld')
    parser_adopt.add_argument(
        '--skip-pull',
        action='store_true',
        help='do not pull the default image before adopting')
    parser_adopt.add_argument(
        '--force-start',
        action='store_true',
        help='start newly adopted daemon, even if it was not running previously')
    parser_adopt.add_argument(
        '--container-init',
        action='store_true',
        default=CONTAINER_INIT,
        help=argparse.SUPPRESS)

    parser_rm_daemon = subparsers.add_parser(
        'rm-daemon', help='remove daemon instance')
    parser_rm_daemon.set_defaults(func=command_rm_daemon)
    parser_rm_daemon.add_argument(
        '--name', '-n',
        required=True,
        action=CustomValidation,
        help='daemon name (type.id)')
    parser_rm_daemon.add_argument(
        '--tcp-ports',
        help='List of tcp ports to close in the host firewall')
    parser_rm_daemon.add_argument(
        '--fsid',
        required=True,
        help='cluster FSID')
    parser_rm_daemon.add_argument(
        '--force',
        action='store_true',
        help='proceed, even though this may destroy valuable data')
    parser_rm_daemon.add_argument(
        '--force-delete-data',
        action='store_true',
        help='delete valuable daemon data instead of making a backup')

    parser_rm_cluster = subparsers.add_parser(
        'rm-cluster', help='remove all daemons for a cluster')
    parser_rm_cluster.set_defaults(func=command_rm_cluster)
    parser_rm_cluster.add_argument(
        '--fsid',
        required=True,
        help='cluster FSID')
    parser_rm_cluster.add_argument(
        '--force',
        action='store_true',
        help='proceed, even though this may destroy valuable data')
    parser_rm_cluster.add_argument(
        '--keep-logs',
        action='store_true',
        help='do not remove log files')
    parser_rm_cluster.add_argument(
        '--zap-osds',
        action='store_true',
        help='zap OSD devices for this cluster')

    parser_run = subparsers.add_parser(
        'run', help='run a ceph daemon, in a container, in the foreground')
    parser_run.set_defaults(func=command_run)
    parser_run.add_argument(
        '--name', '-n',
        required=True,
        help='daemon name (type.id)')
    parser_run.add_argument(
        '--fsid',
        required=True,
        help='cluster FSID')

    parser_shell = subparsers.add_parser(
        'shell', help='run an interactive shell inside a daemon container')
    parser_shell.set_defaults(func=command_shell)
    parser_shell.add_argument(
        '--shared_ceph_folder',
        metavar='CEPH_SOURCE_FOLDER',
        help='Development mode. Several folders in containers are volumes mapped to different sub-folders in the ceph source folder')
    parser_shell.add_argument(
        '--fsid',
        help='cluster FSID')
    parser_shell.add_argument(
        '--name', '-n',
        help='daemon name (type.id)')
    parser_shell.add_argument(
        '--config', '-c',
        help='ceph.conf to pass through to the container')
    parser_shell.add_argument(
        '--keyring', '-k',
        help='ceph.keyring to pass through to the container')
    parser_shell.add_argument(
        '--mount', '-m',
        help=('mount a file or directory in the container. '
              'Support multiple mounts. '
              'ie: `--mount /foo /bar:/bar`. '
              'When no destination is passed, default is /mnt'),
        nargs='+')
    parser_shell.add_argument(
        '--env', '-e',
        action='append',
        default=[],
        help='set environment variable')
    parser_shell.add_argument(
        '--volume', '-v',
        action='append',
        default=[],
        help='set environment variable')
    parser_shell.add_argument(
        'command', nargs=argparse.REMAINDER,
        help='command (optional)')
    parser_shell.add_argument(
        '--no-hosts',
        action='store_true',
        help='dont pass /etc/hosts through to the container')
    parser_shell.add_argument(
        '--dry-run',
        action='store_true',
        help='print, but do not execute, the container command to start the shell')

    parser_enter = subparsers.add_parser(
        'enter', help='run an interactive shell inside a running daemon container')
    parser_enter.set_defaults(func=command_enter)
    parser_enter.add_argument(
        '--fsid',
        help='cluster FSID')
    parser_enter.add_argument(
        '--name', '-n',
        required=True,
        help='daemon name (type.id)')
    parser_enter.add_argument(
        'command', nargs=argparse.REMAINDER,
        help='command')

    parser_ceph_volume = subparsers.add_parser(
        'ceph-volume', help='run ceph-volume inside a container')
    parser_ceph_volume.set_defaults(func=command_ceph_volume)
    parser_ceph_volume.add_argument(
        '--shared_ceph_folder',
        metavar='CEPH_SOURCE_FOLDER',
        help='Development mode. Several folders in containers are volumes mapped to different sub-folders in the ceph source folder')
    parser_ceph_volume.add_argument(
        '--fsid',
        help='cluster FSID')
    parser_ceph_volume.add_argument(
        '--config-json',
        help='JSON file with config and (client.bootstrap-osd) key')
    parser_ceph_volume.add_argument(
        '--config', '-c',
        help='ceph conf file')
    parser_ceph_volume.add_argument(
        '--keyring', '-k',
        help='ceph.keyring to pass through to the container')
    parser_ceph_volume.add_argument(
        'command', nargs=argparse.REMAINDER,
        help='command')

    parser_zap_osds = subparsers.add_parser(
        'zap-osds', help='zap all OSDs associated with a particular fsid')
    parser_zap_osds.set_defaults(func=command_zap_osds)
    parser_zap_osds.add_argument(
        '--fsid',
        required=True,
        help='cluster FSID')
    parser_zap_osds.add_argument(
        '--force',
        action='store_true',
        help='proceed, even though this may destroy valuable data')

    parser_unit = subparsers.add_parser(
        'unit', help="operate on the daemon's systemd unit")
    parser_unit.set_defaults(func=command_unit)
    parser_unit.add_argument(
        'command',
        help='systemd command (start, stop, restart, enable, disable, ...)')
    parser_unit.add_argument(
        '--fsid',
        help='cluster FSID')
    parser_unit.add_argument(
        '--name', '-n',
        required=True,
        help='daemon name (type.id)')

    parser_logs = subparsers.add_parser(
        'logs', help='print journald logs for a daemon container')
    parser_logs.set_defaults(func=command_logs)
    parser_logs.add_argument(
        '--fsid',
        help='cluster FSID')
    parser_logs.add_argument(
        '--name', '-n',
        required=True,
        help='daemon name (type.id)')
    parser_logs.add_argument(
        'command', nargs='*',
        help='additional journalctl args')

    parser_bootstrap = subparsers.add_parser(
        'bootstrap', help='bootstrap a cluster (mon + mgr daemons)')
    parser_bootstrap.set_defaults(func=command_bootstrap)
    parser_bootstrap.add_argument(
        '--config', '-c',
        help='ceph conf file to incorporate')
    parser_bootstrap.add_argument(
        '--mon-id',
        required=False,
        help='mon id (default: local hostname)')
    group = parser_bootstrap.add_mutually_exclusive_group()
    group.add_argument(
        '--mon-addrv',
        help='mon IPs (e.g., [v2:localipaddr:3300,v1:localipaddr:6789])')
    group.add_argument(
        '--mon-ip',
        help='mon IP')
    parser_bootstrap.add_argument(
        '--mgr-id',
        required=False,
        help='mgr id (default: randomly generated)')
    parser_bootstrap.add_argument(
        '--fsid',
        help='cluster FSID')
    parser_bootstrap.add_argument(
        '--output-dir',
        default='/etc/ceph',
        help='directory to write config, keyring, and pub key files')
    parser_bootstrap.add_argument(
        '--output-keyring',
        help='location to write keyring file with new cluster admin and mon keys')
    parser_bootstrap.add_argument(
        '--output-config',
        help='location to write conf file to connect to new cluster')
    parser_bootstrap.add_argument(
        '--output-pub-ssh-key',
        help="location to write the cluster's public SSH key")
    parser_bootstrap.add_argument(
        '--skip-admin-label',
        action='store_true',
        help='do not create admin label for ceph.conf and client.admin keyring distribution')
    parser_bootstrap.add_argument(
        '--skip-ssh',
        action='store_true',
        help='skip setup of ssh key on local host')
    parser_bootstrap.add_argument(
        '--initial-dashboard-user',
        default='admin',
        help='Initial user for the dashboard')
    parser_bootstrap.add_argument(
        '--initial-dashboard-password',
        help='Initial password for the initial dashboard user')
    parser_bootstrap.add_argument(
        '--ssl-dashboard-port',
        type=int,
        default=8443,
        help='Port number used to connect with dashboard using SSL')
    parser_bootstrap.add_argument(
        '--dashboard-key',
        type=argparse.FileType('r'),
        help='Dashboard key')
    parser_bootstrap.add_argument(
        '--dashboard-crt',
        type=argparse.FileType('r'),
        help='Dashboard certificate')

    parser_bootstrap.add_argument(
        '--ssh-config',
        type=argparse.FileType('r'),
        help='SSH config')
    parser_bootstrap.add_argument(
        '--ssh-private-key',
        type=argparse.FileType('r'),
        help='SSH private key')
    parser_bootstrap.add_argument(
        '--ssh-public-key',
        type=argparse.FileType('r'),
        help='SSH public key')
    parser_bootstrap.add_argument(
        '--ssh-signed-cert',
        type=argparse.FileType('r'),
        help='Signed cert for setups using CA signed SSH keys')
    parser_bootstrap.add_argument(
        '--ssh-user',
        default='root',
        help='set user for SSHing to cluster hosts, passwordless sudo will be needed for non-root users')
    parser_bootstrap.add_argument(
        '--skip-mon-network',
        action='store_true',
        help='set mon public_network based on bootstrap mon ip')
    parser_bootstrap.add_argument(
        '--skip-dashboard',
        action='store_true',
        help='do not enable the Ceph Dashboard')
    parser_bootstrap.add_argument(
        '--dashboard-password-noupdate',
        action='store_true',
        help='stop forced dashboard password change')
    parser_bootstrap.add_argument(
        '--no-minimize-config',
        action='store_true',
        help='do not assimilate and minimize the config file')
    parser_bootstrap.add_argument(
        '--skip-ping-check',
        action='store_true',
        help='do not verify that mon IP is pingable')
    parser_bootstrap.add_argument(
        '--skip-pull',
        action='store_true',
        help='do not pull the default image before bootstrapping')
    parser_bootstrap.add_argument(
        '--skip-firewalld',
        action='store_true',
        help='Do not configure firewalld')
    parser_bootstrap.add_argument(
        '--allow-overwrite',
        action='store_true',
        help='allow overwrite of existing --output-* config/keyring/ssh files')
    parser_bootstrap.add_argument(
        '--cleanup-on-failure',
        action='store_true',
        default=False,
        help='Delete cluster files in case of a failed installation')
    parser_bootstrap.add_argument(
        '--allow-fqdn-hostname',
        action='store_true',
        help='allow hostname that is fully-qualified (contains ".")')
    parser_bootstrap.add_argument(
        '--allow-mismatched-release',
        action='store_true',
        help="allow bootstrap of ceph that doesn't match this version of cephadm")
    parser_bootstrap.add_argument(
        '--skip-prepare-host',
        action='store_true',
        help='Do not prepare host')
    parser_bootstrap.add_argument(
        '--orphan-initial-daemons',
        action='store_true',
        help='Set mon and mgr service to `unmanaged`, Do not create the crash service')
    parser_bootstrap.add_argument(
        '--skip-monitoring-stack',
        action='store_true',
        help='Do not automatically provision monitoring stack (prometheus, grafana, alertmanager, node-exporter)')
    parser_bootstrap.add_argument(
        '--with-centralized-logging',
        action='store_true',
        help='Automatically provision centralized logging (promtail, loki)')
    parser_bootstrap.add_argument(
        '--apply-spec',
        help='Apply cluster spec after bootstrap (copy ssh key, add hosts and apply services)')
    parser_bootstrap.add_argument(
        '--shared_ceph_folder',
        metavar='CEPH_SOURCE_FOLDER',
        help='Development mode. Several folders in containers are volumes mapped to different sub-folders in the ceph source folder')

    parser_bootstrap.add_argument(
        '--registry-url',
        help='url for custom registry')
    parser_bootstrap.add_argument(
        '--registry-username',
        help='username for custom registry')
    parser_bootstrap.add_argument(
        '--registry-password',
        help='password for custom registry')
    parser_bootstrap.add_argument(
        '--registry-json',
        help='json file with custom registry login info (URL, Username, Password)')
    parser_bootstrap.add_argument(
        '--container-init',
        action='store_true',
        default=CONTAINER_INIT,
        help=argparse.SUPPRESS)
    parser_bootstrap.add_argument(
        '--cluster-network',
        help='subnet to use for cluster replication, recovery and heartbeats (in CIDR notation network/mask)')
    parser_bootstrap.add_argument(
        '--single-host-defaults',
        action='store_true',
        help='adjust configuration defaults to suit a single-host cluster')
    parser_bootstrap.add_argument(
        '--log-to-file',
        action='store_true',
        help='configure cluster to log to traditional log files in /var/log/ceph/$fsid')

    parser_deploy = subparsers.add_parser(
        'deploy', help='deploy a daemon')
    parser_deploy.set_defaults(func=command_deploy)
    parser_deploy.add_argument(
        '--name',
        required=True,
        action=CustomValidation,
        help='daemon name (type.id)')
    parser_deploy.add_argument(
        '--fsid',
        required=True,
        help='cluster FSID')
    _add_deploy_parser_args(parser_deploy)

    parser_orch = subparsers.add_parser(
        '_orch',
    )
    subparsers_orch = parser_orch.add_subparsers(
        title='Orchestrator Driven Commands',
        description='Commands that are typically only run by cephadm mgr module',
    )

    parser_deploy_from = subparsers_orch.add_parser(
        'deploy', help='deploy a daemon')
    parser_deploy_from.set_defaults(func=command_deploy_from)
    # currently cephadm mgr module passes an fsid option on the CLI too
    # TODO: remove this and always source fsid from the JSON?
    parser_deploy_from.add_argument(
        '--fsid',
        help='cluster FSID')
    parser_deploy_from.add_argument(
        'source',
        default='-',
        nargs='?',
        help='Configuration input source file',
    )

    parser_check_host = subparsers.add_parser(
        'check-host', help='check host configuration')
    parser_check_host.set_defaults(func=command_check_host)
    parser_check_host.add_argument(
        '--expect-hostname',
        help='Check that hostname matches an expected value')

    parser_prepare_host = subparsers.add_parser(
        'prepare-host', help='prepare a host for cephadm use')
    parser_prepare_host.set_defaults(func=command_prepare_host)
    parser_prepare_host.add_argument(
        '--expect-hostname',
        help='Set hostname')

    parser_add_repo = subparsers.add_parser(
        'add-repo', help='configure package repository')
    parser_add_repo.set_defaults(func=command_add_repo)
    parser_add_repo.add_argument(
        '--release',
        help='use latest version of a named release (e.g., {})'.format(LATEST_STABLE_RELEASE))
    parser_add_repo.add_argument(
        '--version',
        help='use specific upstream version (x.y.z)')
    parser_add_repo.add_argument(
        '--dev',
        help='use specified bleeding edge build from git branch or tag')
    parser_add_repo.add_argument(
        '--dev-commit',
        help='use specified bleeding edge build from git commit')
    parser_add_repo.add_argument(
        '--gpg-url',
        help='specify alternative GPG key location')
    parser_add_repo.add_argument(
        '--repo-url',
        default='https://download.ceph.com',
        help='specify alternative repo location')
    # TODO: proxy?

    parser_rm_repo = subparsers.add_parser(
        'rm-repo', help='remove package repository configuration')
    parser_rm_repo.set_defaults(func=command_rm_repo)

    parser_install = subparsers.add_parser(
        'install', help='install ceph package(s)')
    parser_install.set_defaults(func=command_install)
    parser_install.add_argument(
        'packages', nargs='*',
        default=['cephadm'],
        help='packages')

    parser_registry_login = subparsers.add_parser(
        'registry-login', help='log host into authenticated registry')
    parser_registry_login.set_defaults(func=command_registry_login)
    parser_registry_login.add_argument(
        '--registry-url',
        help='url for custom registry')
    parser_registry_login.add_argument(
        '--registry-username',
        help='username for custom registry')
    parser_registry_login.add_argument(
        '--registry-password',
        help='password for custom registry')
    parser_registry_login.add_argument(
        '--registry-json',
        help='json file with custom registry login info (URL, Username, Password)')
    parser_registry_login.add_argument(
        '--fsid',
        help='cluster FSID')

    parser_gather_facts = subparsers.add_parser(
        'gather-facts', help='gather and return host related information (JSON format)')
    parser_gather_facts.set_defaults(func=command_gather_facts)

    parser_maintenance = subparsers.add_parser(
        'host-maintenance', help='Manage the maintenance state of a host')
    parser_maintenance.add_argument(
        '--fsid',
        help='cluster FSID')
    parser_maintenance.add_argument(
        'maintenance_action',
        type=str,
        choices=['enter', 'exit'],
        help='Maintenance action - enter maintenance, or exit maintenance')
    parser_maintenance.set_defaults(func=command_maintenance)

    parser_agent = subparsers.add_parser(
        'agent', help='start cephadm agent')
    parser_agent.set_defaults(func=command_agent)
    parser_agent.add_argument(
        '--fsid',
        required=True,
        help='cluster FSID')
    parser_agent.add_argument(
        '--daemon-id',
        help='daemon id for agent')

    parser_disk_rescan = subparsers.add_parser(
        'disk-rescan', help='rescan all HBAs to detect new/removed devices')
    parser_disk_rescan.set_defaults(func=command_rescan_disks)

    return parser


def _parse_args(av: List[str]) -> argparse.Namespace:
    parser = _get_parser()

    args = parser.parse_args(av)
    if 'command' in args and args.command and args.command[0] == '--':
        args.command.pop(0)

    # workaround argparse to deprecate the subparser `--container-init` flag
    # container_init and no_container_init must always be mutually exclusive
    container_init_args = ('--container-init', '--no-container-init')
    if set(container_init_args).issubset(av):
        parser.error('argument %s: not allowed with argument %s' % (container_init_args))
    elif '--container-init' in av:
        args.no_container_init = not args.container_init
    else:
        args.container_init = not args.no_container_init
    assert args.container_init is not args.no_container_init

    return args


def cephadm_init_ctx(args: List[str]) -> CephadmContext:
    ctx = CephadmContext()
    ctx.set_args(_parse_args(args))
    return ctx


def cephadm_init_logging(ctx: CephadmContext, args: List[str]) -> None:
    """Configure the logging for cephadm as well as updating the system
    to have the expected log dir and logrotate configuration.
    """
    logging.addLevelName(QUIET_LOG_LEVEL, 'QUIET')
    global logger
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    operations = ['bootstrap', 'rm-cluster']
    if any(op in args for op in operations):
        dictConfig(interactive_logging_config)
    else:
        dictConfig(logging_config)

    logger = logging.getLogger()
    logger.setLevel(QUIET_LOG_LEVEL)

    if not os.path.exists(ctx.logrotate_dir + '/cephadm'):
        with open(ctx.logrotate_dir + '/cephadm', 'w') as f:
            f.write("""# created by cephadm
/var/log/ceph/cephadm.log {
    rotate 7
    daily
    compress
    missingok
    notifempty
    su root root
}
""")

    if ctx.verbose:
        for handler in logger.handlers:
            if handler.name in ['console', 'log_file', 'console_stdout']:
                handler.setLevel(QUIET_LOG_LEVEL)
    logger.debug('%s\ncephadm %s' % ('-' * 80, args))


def cephadm_require_root() -> None:
    """Exit if the process is not running as root."""
    if os.geteuid() != 0:
        sys.stderr.write('ERROR: cephadm should be run as root\n')
        sys.exit(1)


def main() -> None:
    av: List[str] = []
    av = sys.argv[1:]

    ctx = cephadm_init_ctx(av)
    if not ctx.has_function():
        sys.stderr.write('No command specified; pass -h or --help for usage\n')
        sys.exit(1)

    if ctx.has_function() and getattr(ctx.func, '_execute_early', False):
        try:
            sys.exit(ctx.func(ctx))
        except Error as e:
            if ctx.verbose:
                raise
            logger.error('ERROR: %s' % e)
            sys.exit(1)

    cephadm_require_root()
    cephadm_init_logging(ctx, av)
    try:
        # podman or docker?
        ctx.container_engine = find_container_engine(ctx)
        if ctx.func not in \
                [
                    command_check_host,
                    command_prepare_host,
                    command_add_repo,
                    command_rm_repo,
                    command_install
                ]:
            check_container_engine(ctx)
        # command handler
        r = ctx.func(ctx)
    except (Error, ClusterAlreadyExists) as e:
        if ctx.verbose:
            raise
        logger.error('ERROR: %s' % e)
        sys.exit(1)
    if not r:
        r = 0
    sys.exit(r)


if __name__ == '__main__':
    main()
