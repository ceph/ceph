import argparse
import configparser
import contextlib
import json
import logging
import os
import pathlib
import re
import shlex
import shutil
import sys
import tempfile

from typing import List, Any, Optional, Callable, Iterable, TypedDict, IO

import yaml

from cephadmlib.constants import CUSTOM_PS1
from cephadmlib.call_wrappers import call, call_timeout
from cephadmlib.container_engines import (
    check_container_engine,
    find_container_engine,
)
from cephadmlib.container_types import CephContainer
from cephadmlib.context import CephadmContext
from cephadmlib.exceptions import Error
from cephadmlib.file_utils import pathify, write_new


logger = logging.getLogger()


_CLUSTERS = 'clusters.yaml'
_UUID_RE = (
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-5][0-9a-f]{3}-[089ab][0-9a-f]{3}'
    r'-[0-9a-f]{12}$'
)


def _config_root(ctx: CephadmContext) -> pathlib.Path:
    config_root = getattr(ctx, 'config_root', None)
    if config_root:
        return pathlib.Path(config_root).absolute()
    cfg_home = os.environ.get('XDG_CONFIG_HOME')
    if not cfg_home:
        cfg_home = pathlib.Path('~/.config').expanduser()
    cfg_home = pathlib.Path(cfg_home).absolute()
    return cfg_home / 'ceph/uca'


def _load_clusters(ctx: CephadmContext) -> None:
    fsids: List[str] = []
    fsidre = re.compile(_UUID_RE)
    for entry in ctx.config_dir.iterdir():
        if entry.is_dir() and fsidre.match(entry.name):
            fsids.append(entry.name)
    ctx.clusters = fsids


def _load_clusters_meta(ctx: CephadmContext) -> None:
    path = ctx.config_dir / _CLUSTERS
    try:
        with open(path, 'r') as fh:
            data = yaml.safe_load(fh)
    except FileNotFoundError:
        return
    ctx.default_cluster = str(data.get('default_cluster', ''))
    if _aliases := data.get('aliases'):
        if not isinstance(_aliases, dict):
            raise ValueError('aliases is not a mapping')
        ctx.cluster_aliases = {str(k): str(v) for k, v in _aliases.items()}


def _write_clusters_meta(ctx: CephadmContext) -> None:
    new_conf = {}
    if ctx.default_cluster:
        new_conf['default_cluster'] = ctx.default_cluster
    new_conf['aliases'] = ctx.cluster_aliases or []
    path = ctx.config_dir / _CLUSTERS
    with write_new(path, perms=None) as cmeta:
        yaml.safe_dump(new_conf, cmeta)


class KeyringIdentity(TypedDict):
    name: str
    key: str


class ClusterConfig:
    fsid: str = ''
    _root: pathlib.Path

    # configuration data fields
    _identities: List[KeyringIdentity] = []
    _mon_host: str = ''
    _image: str = ''

    def _load_conf(self):
        with open(self.cluster_conf_path, 'r') as fh:
            data = yaml.safe_load(fh) or {}
        if self.fsid != data.get('fsid'):
            raise ValueError('invalid fsid in configuration file')
        if data.get('version', 0) != 0:
            raise ValueError('invalid version in configuration file')
        self._mon_host = data.get('mon_host', '')
        if not self._mon_host:
            raise ValueError('missing mon host configuration')
        self._identities = [
            KeyringIdentity(d) for d in (data.get('identities') or ())
        ]
        self._image = data.get('image', '')

    def _save_conf(self):
        new_conf = {
            'fsid': self.fsid,
            'version': 0,
            'mon_host': self._mon_host,
            'identities': self._identities,
            'image': self._image,
        }
        with write_new(self.cluster_conf_path, perms=None) as fh:
            yaml.safe_dump(new_conf, fh)

    def save(self):
        """Save current cluster configuration to disk."""
        self._save_conf()

    def import_ceph_conf(
        self,
        *,
        conf_path: Optional[pathlib.Path] = None,
        conf_data: str = '',
    ) -> None:
        """Import config params from a ceph.conf-style file."""
        cparser = configparser.ConfigParser()
        if conf_path:
            cparser.read(conf_path)
        else:
            cparser.read_string(conf_data)
        if not cparser.has_section('global'):
            raise ValueError('can not import config: no global section')
        conf_fsid = cparser['global']['fsid']
        mon_host = cparser['global']['mon_host']
        if conf_fsid != self.fsid:
            raise ValueError('cluster fsid and conf fsid mismatch')
        self._mon_host = mon_host

    def import_ceph_keyring(
        self,
        *,
        keyring_path: Optional[pathlib.Path] = None,
        keyring_data: str = '',
    ) -> None:
        """Import config params from a ceph keyring file."""
        cparser = configparser.ConfigParser()
        if keyring_path:
            cparser.read(keyring_path)
        else:
            cparser.read_string(keyring_data)
        identities = []
        for section in cparser.sections():
            info: KeyringIdentity = {}
            info['name'] = section
            info['key'] = cparser[section]['key']
            identities.append(info)
        self._identities = identities

    def _build_ceph_conf(self) -> configparser.ConfigParser:
        cparser = configparser.ConfigParser()
        cparser['global'] = {
            'fsid': self.fsid,
            'mon_host': self._mon_host,
        }
        return cparser

    def _build_ceph_keyring(self) -> configparser.ConfigParser:
        cparser = configparser.ConfigParser()
        for ident in self._identities:
            name = ident['name']
            key = ident['key']
            cparser[name] = {'key': key}
        return cparser

    def write_ceph_conf(self, fh: IO) -> None:
        """Write config data to a ceph.conf-style file."""
        self._build_ceph_conf().write(fh)
        fh.flush()  # flush to ensure data is on disk w/o close

    def write_ceph_keyring(self, fh: IO) -> None:
        """Write keyring data to a ceph keyring style file."""
        self._build_ceph_keyring().write(fh)
        fh.flush()  # flush to ensure data is on disk w/o close

    @property
    def config_dir(self) -> pathlib.Path:
        assert self.fsid
        return self._root / self.fsid

    @property
    def cluster_conf_path(self) -> pathlib.Path:
        return self.config_dir / 'cluster.yaml'

    @property
    def vhome(self) -> Optional[pathlib.Path]:
        path = self.config_dir / 'home'
        return path if path.is_dir() else None

    @property
    def image(self) -> str:
        return self._image or 'quay.ceph.io/ceph-ci/ceph:main'

    def history_file(self) -> Optional[pathlib.Path]:
        path = self.config_dir / 'history'
        try:
            if not path.exists():
                path.write_text('')
        except IOError:
            return None
        return path

    @classmethod
    def load(cls, ctx: CephadmContext, cluster: str) -> 'ClusterConfig':
        """Create a load a normally configured cluster."""
        cc = cls()
        cc._root = ctx.config_dir
        if cluster in ctx.cluster_aliases:
            cc.fsid = ctx.cluster_aliases[cluster]
        elif cluster in ctx.clusters:
            cc.fsid = cluster
        else:
            raise Error(f'cluster {cluster} not found')
        cc._load_conf()
        return cc

    @classmethod
    def new(cls, ctx: CephadmContext, cluster: str) -> 'ClusterConfig':
        """Create an unloaded cluster config object."""
        cc = cls()
        cc._root = ctx.config_dir
        cc.fsid = cluster
        return cc

    @classmethod
    def empty(cls, ctx: CephadmContext) -> 'ClusterConfig':
        """Create a completely empty cluster config object."""
        cc = cls()
        cc._root = ctx.config_dir
        return cc


def configure_uca(ctx: CephadmContext, *, init: bool = False) -> None:
    ctx.uca_configured = False
    ctx.clusters: List[str] = []
    ctx.cluster_aliases: Dict[str, str] = {}
    ctx.default_cluster = ''
    ctx.config_dir = _config_root(ctx)
    if init:
        ctx.config_dir.mkdir(parents=True, exist_ok=True)
    ctx.current_cluster = ClusterConfig.empty(ctx)
    if not ctx.config_dir.is_dir():
        return
    _load_clusters(ctx)
    _load_clusters_meta(ctx)
    cluster_choice = ctx.cluster or ctx.default_cluster
    ctx.uca_configured = True
    if cluster_choice:
        ctx.current_cluster = ClusterConfig.load(ctx, cluster_choice)


def require_configuration(ctx: CephadmContext) -> None:
    if not ctx.uca_configured:
        raise Error(
            'no uca configuration found'
            ' (use `uca cluster init` or `uca cluster ssh-init` to create one)'
        )


def require_cluster(ctx: CephadmContext) -> None:
    require_configuration(ctx)
    if not ctx.current_cluster.fsid:
        raise Error('no cluster specified')


def command_version(ctx: CephadmContext) -> int:
    """Report uca version information."""
    import importlib
    import zipimport
    import types

    vmod: Optional[types.ModuleType]
    zmod: Optional[types.ModuleType]
    try:
        vmod = importlib.import_module('_cephadmmeta.version')
        zmod = vmod
    except ImportError:
        vmod = zmod = None
    if zmod is None:
        # fallback to outer package, for zip import module
        try:
            zmod = importlib.import_module('_cephadmmeta')
        except ImportError:
            zmod = None

    if not ctx.verbose:
        if vmod is None:
            print('uca version UNKNOWN')
            return 1
        _unset = '<UNSET>'
        print(
            'uca version {0} ({1}) {2} ({3})'.format(
                getattr(vmod, 'CEPH_GIT_NICE_VER', _unset),
                getattr(vmod, 'CEPH_GIT_VER', _unset),
                getattr(vmod, 'CEPH_RELEASE_NAME', _unset),
                getattr(vmod, 'CEPH_RELEASE_TYPE', _unset),
            )
        )
        return 0

    out: Dict[str, Any] = {'name': 'uca'}
    ceph_vars = [
        'CEPH_GIT_NICE_VER',
        'CEPH_GIT_VER',
        'CEPH_RELEASE_NAME',
        'CEPH_RELEASE_TYPE',
    ]
    for var in ceph_vars:
        value = getattr(vmod, var, None)
        if value is not None:
            out[var.lower()] = value

    loader = getattr(zmod, '__loader__', None)
    if loader and isinstance(loader, zipimport.zipimporter):
        try:
            deps_info = json.loads(loader.get_data('_cephadmmeta/deps.json'))
            out['bundled_packages'] = deps_info
        except OSError:
            pass
        files = getattr(loader, '_files', {})
        out['zip_root_entries'] = sorted(
            {p.split('/')[0] for p in files.keys()}
        )

    json.dump(out, sys.stdout, indent=2)
    print()
    return 0


def command_shell(ctx: CephadmContext) -> int:
    """Run an interactive shell inside a ceph container."""
    configure_uca(ctx)
    require_cluster(ctx)
    return _shell(ctx, ctx.command)


def command_ceph(ctx: CephadmContext) -> int:
    """Execute a ceph command inside a ceph container."""
    configure_uca(ctx)
    require_cluster(ctx)

    command = ['ceph']
    # use argparse to intercept -i options so we can map files on the "host"
    # into the container.
    vparser = argparse.ArgumentParser(add_help=False, exit_on_error=False)
    vparser.add_argument('-i', dest='inbuf')
    if ctx.command[0] == '--':
        _command = ctx.command[1:]
    else:
        _command = ctx.command
    vargs, other_args = vparser.parse_known_args(_command)
    command += other_args

    vols = getattr(ctx, 'volume', None) or []
    if vargs.inbuf and vargs.inbuf != '-':
        host_path = pathlib.Path(vargs.inbuf).absolute()
        # map paths into /var/tmp to avoid silly breakages like trying to map
        # over container files like /usr/bin/ceph or /etc/passwd or something
        # along those lines. Not for security, but just for robustness.
        ctr_path = pathlib.Path('/var/tmp') / host_path.relative_to('/')
        vols.append(f'{host_path}:{ctr_path}:ro')
        command += ['-i', str(ctr_path)]
    elif vargs.inbuf == '-':
        command += ['-i', '-']
    ctx.volume = vols

    return _shell(ctx, command)


def _shell(ctx: CephadmContext, command: Optional[List[str]] = None) -> int:
    container_args: List[str] = ['-i']
    mounts: Dict[str, str] = {}
    binds: List[str] = []

    if command:
        command = command
    else:
        command = ['bash']
        container_args += [
            '-t',
            '-e',
            'LANG=C',
            '-e',
            'PS1=%s' % CUSTOM_PS1,
        ]
        if vhome := ctx.current_cluster.vhome:
            mounts[pathify(vhome)] = '/root'
        elif history := ctx.current_cluster.history_file():
            mounts[pathify(history)] = '/root/.bash_history'

    for vol in ctx.volume:
        local, mapped = vol.split(':', 1)
        mounts[local] = mapped

    with contextlib.ExitStack() as stack:
        tmp1 = stack.enter_context(
            tempfile.NamedTemporaryFile(mode='w+', prefix='uca-tmp')
        )
        ctx.current_cluster.write_ceph_conf(tmp1)
        mounts[tmp1.name] = '/etc/ceph/ceph.conf:z'
        tmp2 = stack.enter_context(
            tempfile.NamedTemporaryFile(mode='w+', prefix='uca-tmp')
        )
        ctx.current_cluster.write_ceph_keyring(tmp2)
        mounts[tmp2.name] = '/etc/ceph/ceph.keyring:z'

        ctr = CephContainer(
            ctx,
            image=ctx.image or ctx.current_cluster.image,
            entrypoint='doesnotmatter',
            args=[],
            container_args=container_args,
            volume_mounts=mounts,
            bind_mounts=binds,
            envs=ctx.env,
            privileged=False,
        )
        command = ctr.shell_cmd(command)

        if ctx.dry_run:
            print(' '.join(shlex.quote(arg) for arg in command))
            return 0
        return call_timeout(ctx, command, ctx.timeout)


def _init_cluster(
    ctx: CephadmContext,
    to_create: str,
    *,
    conf_path: Optional[pathlib.Path] = None,
    keyring_path: Optional[pathlib.Path] = None,
    conf_data: str = '',
    keyring_data: str = '',
) -> None:
    # create config for cluster
    tmpc = ClusterConfig.new(ctx, to_create)
    tmpc.cluster_conf_path.parent.mkdir(parents=True, exist_ok=True)
    tmpc.import_ceph_conf(conf_path=conf_path, conf_data=conf_data)
    tmpc.import_ceph_keyring(
        keyring_path=keyring_path, keyring_data=keyring_data
    )
    tmpc.save()

    # update metadata
    if to_create not in ctx.clusters:
        ctx.clusters.append(to_create)
    if ctx.make_default:
        ctx.default_cluster = to_create
    for alias in ctx.new_alias or []:
        ctx.cluster_aliases[alias] = to_create
    _write_clusters_meta(ctx)


def command_cluster_init(ctx: CephadmContext) -> None:
    """Initialize a new cluster configuration."""
    configure_uca(ctx, init=True)
    require_configuration(ctx)

    to_create = ctx.CLUSTER or ctx.cluster or ''
    if not to_create:
        raise Error('specify a cluster FSID to initialize a cluster')
    fsidre = re.compile(_UUID_RE)
    if not fsidre.match(to_create):
        raise Error(f'FSID must be in the form of a UUID, not {to_create!r}')
    if not ctx.conf:
        raise Error('a ceph config is required')
    if not ctx.keyring:
        raise Error('a ceph keyring is required')
    _init_cluster(
        ctx, to_create, conf_path=ctx.conf, keyring_path=ctx.keyring
    )


def command_cluster_ssh_init(ctx: CephadmContext) -> None:
    """Initialize a cluster config by sshing into an admin host."""
    configure_uca(ctx, init=True)
    require_configuration(ctx)

    ssh_prefix = ['ssh']
    if ctx.ssh_config_file:
        ssh_prefix += ['-F', str(ctx.ssh_config_file)]
    if ctx.login_name:
        ssh_prefix.append(f'-l{ctx.login_name}')
    if ctx.ssh_extra_args:
        ssh_prefix += ctx.ssh_extra_args
    ssh_prefix.append(ctx.HOST)

    print('Probing host...')
    probes = [
        (['ceph', '-h'], ['ceph']),
        (['cephadm', '-h'], ['cephadm', 'shell', 'ceph']),
    ]
    command_prefix = None
    for probe, cmd in probes:
        _, _, ret = call(ctx, ssh_prefix + probe, timeout=ctx.timeout)
        if ret == 0:
            command_prefix = ssh_prefix + cmd
            break
    if not command_prefix:
        raise Error('unable to find ceph/cephadm command on remote host')

    print('Getting ceph config...')
    command = command_prefix + ['config', 'generate-minimal-conf']
    stdout, stderr, ret = call(ctx, command)
    if ret != 0:
        print(stderr)
        raise Error('failed to get ceph conf')
    conf_data = stdout

    print('Getting ceph keyring...')
    command = command_prefix + ['auth', 'get', ctx.ceph_entity]
    stdout, stderr, ret = call(ctx, command)
    if ret != 0:
        print(stderr)
        raise Error('failed to get ceph user')
    keyring_data = stdout

    print('Initializing cluster config...')
    cfgp = configparser.ConfigParser()
    cfgp.read_string(conf_data)
    to_create = cfgp.get('global', 'fsid')

    _init_cluster(
        ctx, to_create, conf_data=conf_data, keyring_data=keyring_data
    )


def command_cluster_ls(ctx: CephadmContext) -> None:
    """List configured clusters."""
    configure_uca(ctx)
    require_configuration(ctx)

    cluster_list = []
    for fsid in ctx.clusters:
        aliases = [a for a, f in ctx.cluster_aliases.items() if f == fsid]
        default = fsid == ctx.default_cluster
        cluster_list.append(
            {'fsid': fsid, 'aliases': aliases, 'default': default}
        )

    if ctx.output_format == 'json':
        json.dump(cluster_list, sys.stdout)
        sys.stdout.write('\n')
        return
    for cluster_info in cluster_list:
        line = f'{cluster_info["fsid"]}\t{" ".join(cluster_info["aliases"])}'
        line += ' (default)' if cluster_info['default'] else ''
        print(line)


def command_cluster_rm(ctx: CephadmContext) -> None:
    """Remove a cluster configuration."""
    configure_uca(ctx)
    require_configuration(ctx)

    to_remove = ctx.CLUSTER or ctx.cluster
    if not to_remove:
        raise Error('no cluster to remove was specified')
    if to_remove in ctx.cluster_aliases:
        to_remove = ctx.cluster_aliases[to_remove]
    elif to_remove not in ctx.clusters:
        sys.stderr.write(f'cluster {to_remove} not present\n')
        return 0

    # rm metadata
    if ctx.default_cluster == to_remove:
        ctx.default_cluster = ''
    ctx.cluster_aliases = {
        a: f for a, f in ctx.cluster_aliases.items() if f != to_remove
    }
    _write_clusters_meta(ctx)
    # rm config dir
    cfg_dir = ctx.config_dir / to_remove
    trash_dir = ctx.config_dir / f'.trash-{to_remove}'
    cfg_dir.rename(trash_dir)  # rename is atomic (avoids partial deletes)
    shutil.rmtree(trash_dir)
    # find and remove other lingering trash dirs
    for entry in ctx.config_dir.iterdir():
        if entry.name.startswith('.trash-'):
            shutil.rmtree(entry)


def command_cluster_modify(ctx: CephadmContext) -> None:
    """Modify a cluster configuration."""
    configure_uca(ctx)
    require_configuration(ctx)

    to_modify = ctx.CLUSTER or ctx.cluster
    if not to_modify:
        raise Error('unknown cluster')
    if to_modify in ctx.cluster_aliases:
        to_modify = ctx.cluster_aliases[to_modify]
    elif to_modify not in ctx.clusters:
        raise Error(f'unknown cluster: {to_modify}')

    if ctx.clear_aliases:
        ctx.cluster_aliases = {
            a: f for a, f in ctx.cluster_aliases.items() if f != to_modify
        }
    for rm_alias in ctx.rm_alias or []:
        ctx.cluster_aliases = {
            a: f
            for a, f in ctx.cluster_aliases.items()
            if not (f == to_modify and a == rm_alias)
        }
    for new_alias in ctx.new_alias or []:
        ctx.cluster_aliases[new_alias] = to_modify
    if ctx.make_default:
        ctx.default_cluster = to_modify
    elif ctx.clear_default and ctx.default_cluster == to_modify:
        ctx.default_cluster = ''
    elif ctx.clear_default:
        sys.stderr.write("not the default\n")
    _write_clusters_meta(ctx)


def cephadm_init_ctx(args: List[str]) -> CephadmContext:
    ctx = CephadmContext()
    ctx.set_args(_parse_args(args))
    return ctx


def uca_init_logging(ctx: CephadmContext, logger: Any, av: Any) -> None:
    pass  # TODO


def uca_init_container_engine(ctx: CephadmContext) -> None:
    ctx.container_engine = find_container_engine(ctx)
    func = getattr(ctx, "func", None)
    if func:
        check_container_engine(ctx)


def _get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'uca is a ceph adminstrative tool that can be run on'
            ' systems that are NOT ceph cluster nodes'
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--image',
        help='container image. Can also be set via the "CEPHADM_IMAGE" '
        'env var',
    )
    parser.add_argument(
        '--docker', action='store_true', help='use docker instead of podman'
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Show debug-level log messages',
    )
    parser.add_argument(
        '--cluster',
        '--fsid',
        help='Alias or UUID of cluster',
    )
    parser.add_argument(
        '--config-root', help='Path to a custom configuration root directory'
    )

    subparsers = parser.add_subparsers(help='sub-command')

    with _subcommand(subparsers, 'version', command_version) as sp:
        sp.add_argument(
            '--verbose',
            action='store_true',
            help='Detailed version information',
        )

    with _subcommand(subparsers, 'shell', command_shell) as sp:
        sp.add_argument(
            '--mount',
            '-m',
            help=(
                'mount a file or directory in the container. '
                'Support multiple mounts. '
                'ie: `--mount /foo /bar:/bar`. '
                'When no destination is passed, default is /mnt'
            ),
            nargs='+',
        )
        sp.add_argument(
            '--env',
            '-e',
            action='append',
            default=[],
            help='set environment variable',
        )
        sp.add_argument(
            '--volume',
            '-v',
            action='append',
            default=[],
            help='mount a volume',
        )
        sp.add_argument(
            'command', nargs=argparse.REMAINDER, help='command (optional)'
        )
        sp.add_argument(
            '--no-hosts',
            action='store_true',
            help='dont pass /etc/hosts through to the container',
        )
        sp.add_argument(
            '--dry-run',
            action='store_true',
            help='print, but do not execute, the container command to start the shell',
        )

    with _subcommand(subparsers, 'ceph', command_ceph) as sp:
        sp.add_argument(
            '--dry-run',
            action='store_true',
            help='print, but do not execute, the containerized ceph command',
        )
        sp.add_argument(
            '--no-hosts',
            action='store_true',
            help='dont pass /etc/hosts through to the container',
        )
        sp.add_argument('command', nargs=argparse.REMAINDER, help='command')

    cluster_p = subparsers.add_parser(
        'cluster',
        help='Cluster management sub-commands.',
    )
    cluster_subs = cluster_p.add_subparsers()
    with _subcommand(cluster_subs, 'init', command_cluster_init) as sp:
        sp.add_argument(
            '--conf',
            help='Path to a ceph configuration file',
        )
        sp.add_argument(
            '--keyring',
            help='Path to a ceph keyring file',
        )
        sp.add_argument(
            '--alias',
            '-a',
            dest='new_alias',
            action='append',
            help='Human friendly name for a cluster',
        )
        sp.add_argument(
            '--make-default',
            action='store_true',
            help='Make this cluster the default',
        )
        sp.add_argument(
            'CLUSTER',
            nargs='?',
            help='Cluster FSID (UUID)',
        )
    with _subcommand(cluster_subs, 'ssh-init', command_cluster_ssh_init) as sp:
        sp.add_argument(
            '--ceph-entity',
            default='client.admin',
            help='Fetch keyring for user/entity',
        )
        sp.add_argument(
            '--login-name',
            '-l',
            help='Login name (passed to ssh -l option)',
        )
        sp.add_argument(
            '--ssh-config-file',
            '-F',
            help='ssh configuration file (passed to ssh -F option)',
        )
        sp.add_argument(
            '--ssh-extra-args',
            type=shlex.split,
            help='Other extra arguments to pass to ssh command',
        )
        sp.add_argument(
            '--alias',
            '-a',
            dest='new_alias',
            action='append',
            help='Human friendly name for a cluster',
        )
        sp.add_argument(
            '--make-default',
            action='store_true',
            help='Make this cluster the default',
        )
        sp.add_argument(
            'HOST',
            help='ssh destination - can be host or user@host',
        )
    with _subcommand(cluster_subs, 'ls', command_cluster_ls) as sp:
        sp.add_argument(
            '--format',
            dest='output_format',
            choices=('default', 'json'),
            help='Select output format',
        )
    with _subcommand(cluster_subs, 'rm', command_cluster_rm) as sp:
        sp.add_argument('CLUSTER', nargs='?', help='Cluster to remove')
    with _subcommand(cluster_subs, 'modify', command_cluster_modify) as sp:
        sp.add_argument(
            '--alias',
            '-a',
            dest='new_alias',
            action='append',
            help='Human friendly name for a cluster',
        )
        sp.add_argument(
            '--make-default',
            action='store_true',
            help='Make this cluster the default',
        )
        sp.add_argument(
            '--clear-default',
            action='store_true',
            help='Clear this cluster from being the default',
        )
        sp.add_argument(
            '--rm-alias',
            action='append',
            help='Remove specified alias',
        )
        sp.add_argument(
            '--clear-aliases',
            action='store_true',
            help='Remove all aliases',
        )
        sp.add_argument('CLUSTER', nargs='?', help='Cluster to modify')

    return parser


@contextlib.contextmanager
def _subcommand(
    subparsers: Any, name: str, func: Callable
) -> Iterable[argparse.ArgumentParser]:
    kwargs = {}
    if doc := getattr(func, '__doc__', None):
        kwargs['help'] = doc
    parser = subparsers.add_parser(name, **kwargs)
    parser.set_defaults(func=func)
    yield parser


def _parse_args(av: List[str]) -> argparse.Namespace:
    parser = _get_parser()
    args = parser.parse_args(av)
    return args


@contextlib.contextmanager
def _clean_errors(ctx: CephadmContext) -> Iterable[None]:
    try:
        yield
    except Error as err:
        if ctx.verbose:
            raise
        sys.stderr.write(f'ERROR: {err}\n')
        sys.exit(1)


def uca_init_ctx(args: List[str]) -> CephadmContext:
    ctx = CephadmContext()
    ctx.set_args(_parse_args(args))
    return ctx


def main() -> None:
    av: List[str] = []
    av = sys.argv[1:]

    ctx = uca_init_ctx(av)
    if not ctx.has_function():
        sys.stderr.write(
            'No command specified; pass -h or --help for usage\n'
        )
        sys.exit(1)

    uca_init_logging(ctx, logger, av)
    uca_init_container_engine(ctx)
    with _clean_errors(ctx):
        ret = ctx.func(ctx)
    if ret is None:
        ret = 0
    sys.exit(ret)


if __name__ == '__main__':
    main()
