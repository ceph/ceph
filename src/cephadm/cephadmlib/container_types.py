# container_types.py - container instance wrapper types

import copy
import os

from typing import Dict, List, Optional, Any, Union, Tuple, cast

from .call_wrappers import call, call_throws, CallVerbosity
from .constants import DEFAULT_TIMEOUT
from .container_engines import Docker, Podman
from .context import CephadmContext
from .daemon_identity import DaemonIdentity, DaemonSubIdentity
from .exceptions import Error
from .net_utils import get_hostname


class BasicContainer:
    def __init__(
        self,
        ctx: CephadmContext,
        *,
        image: str,
        entrypoint: str,
        identity: Optional['DaemonIdentity'],
        args: Optional[List[str]] = None,
        container_args: Optional[List[str]] = None,
        envs: Optional[List[str]] = None,
        volume_mounts: Optional[Dict[str, str]] = None,
        bind_mounts: Optional[List[List[str]]] = None,
        network: str = '',
        ipc: str = '',
        init: bool = False,
        ptrace: bool = False,
        privileged: bool = False,
        remove: bool = False,
        memory_request: Optional[str] = None,
        memory_limit: Optional[str] = None,
    ) -> None:
        self.ctx = ctx
        self.image = image
        self.entrypoint = entrypoint
        self.identity = identity
        self.args = args or []
        self.container_args = container_args or []
        self.envs = envs or []
        self.volume_mounts = volume_mounts or {}
        self.bind_mounts = bind_mounts or []
        self.network = network
        self.ipc = ipc
        self.init = init
        self.ptrace = ptrace
        self.privileged = privileged
        self.remove = remove
        self.memory_request = memory_request
        self.memory_limit = memory_limit

    @property
    def _container_engine(self) -> str:
        return self.ctx.container_engine.path

    @property
    def _using_podman(self) -> bool:
        return isinstance(self.ctx.container_engine, Podman)

    @property
    def _using_docker(self) -> bool:
        return isinstance(self.ctx.container_engine, Docker)

    @property
    def cname(self) -> str:
        assert self.identity
        return self.identity.container_name

    def build_engine_run_args(self) -> List[str]:
        cmd_args: List[str] = []
        if self.remove:
            cmd_args.append('--rm')
        if self.ipc:
            cmd_args.append(f'--ipc={self.ipc}')
        # some containers (ahem, haproxy) override this, but we want a fast
        # shutdown always (and, more importantly, a successful exit even if we
        # fall back to SIGKILL).
        cmd_args.append('--stop-signal=SIGTERM')

        if isinstance(self.ctx.container_engine, Podman):
            if os.path.exists('/etc/ceph/podman-auth.json'):
                cmd_args.append('--authfile=/etc/ceph/podman-auth.json')

        if isinstance(self.ctx.container_engine, Docker):
            cmd_args.extend(['--ulimit', 'nofile=1048576'])

        if self.memory_request:
            cmd_args.extend(
                ['-e', 'POD_MEMORY_REQUEST', str(self.memory_request)]
            )
        if self.memory_limit:
            cmd_args.extend(
                ['-e', 'POD_MEMORY_LIMIT', str(self.memory_limit)]
            )
            cmd_args.extend(['--memory', str(self.memory_limit)])

        if self.network:
            cmd_args.append(f'--net={self.network}')
        if self.entrypoint:
            cmd_args.extend(['--entrypoint', self.entrypoint])
        if self.privileged:
            cmd_args.extend(
                [
                    '--privileged',
                    # let OSD etc read block devs that haven't been chowned
                    '--group-add=disk',
                ]
            )
        if self.ptrace and not self.privileged:
            # if privileged, the SYS_PTRACE cap is already added
            # in addition, --cap-add and --privileged are mutually
            # exclusive since podman >= 2.0
            cmd_args.append('--cap-add=SYS_PTRACE')
        if self.init:
            cmd_args.append('--init')
        if self.cname:
            cmd_args.extend(['--name', self.cname])

        envs: List[str] = [
            '-e',
            'CONTAINER_IMAGE=%s' % self.image,
        ]
        if self.envs:
            for env in self.envs:
                envs.extend(['-e', env])

        vols: List[str] = []
        vols = sum(
            [
                ['-v', '%s:%s' % (host_dir, container_dir)]
                for host_dir, container_dir in self.volume_mounts.items()
            ],
            [],
        )

        binds: List[str] = []
        binds = sum(
            [
                ['--mount', '{}'.format(','.join(bind))]
                for bind in self.bind_mounts
            ],
            [],
        )

        return cmd_args + self.container_args + envs + vols + binds

    def build_run_cmd(self) -> List[str]:
        return (
            [self._container_engine, 'run']
            + self.build_engine_run_args()
            + [self.image]
            + list(self.args)
        )

    def build_rm_cmd(
        self, cname: str = '', storage: bool = False
    ) -> List[str]:
        cmd = [
            self._container_engine,
            'rm',
            '-f',
        ]
        if storage:
            cmd.append('--storage')
        cmd.append(cname or self.cname)
        return cmd

    def build_stop_cmd(
        self, cname: str = '', timeout: Optional[int] = None
    ) -> List[str]:
        cmd = [self._container_engine, 'stop']
        if timeout is not None:
            cmd.extend(('-t', str(timeout)))
        cmd.append(cname or self.cname)
        return cmd

    @classmethod
    def from_container(
        cls,
        other: 'BasicContainer',
        *,
        ident: Optional[DaemonIdentity] = None,
    ) -> 'BasicContainer':
        return cls(
            other.ctx,
            image=other.image,
            entrypoint=other.entrypoint,
            identity=(ident or other.identity),
            args=other.args,
            container_args=copy.copy(other.container_args),
            envs=copy.copy(other.envs),
            volume_mounts=copy.copy(other.volume_mounts),
            bind_mounts=copy.copy(other.bind_mounts),
            network=other.network,
            ipc=other.ipc,
            init=other.init,
            ptrace=other.ptrace,
            privileged=other.privileged,
            remove=other.remove,
            memory_request=other.memory_request,
            memory_limit=other.memory_limit,
        )


class CephContainer(BasicContainer):
    def __init__(
        self,
        ctx: CephadmContext,
        image: str,
        entrypoint: str,
        args: List[str] = [],
        volume_mounts: Dict[str, str] = {},
        identity: Optional['DaemonIdentity'] = None,
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
        self.identity = identity
        self._cname = cname
        self.container_args = container_args
        self.envs = envs or []
        self.privileged = privileged
        self.ptrace = ptrace
        self.bind_mounts = bind_mounts if bind_mounts else []
        self.init = init if init else ctx.container_init
        self.host_network = host_network
        self.memory_request = memory_request
        self.memory_limit = memory_limit
        self.remove = True
        self.ipc = 'host'
        self.network = 'host' if self.host_network else ''

    @classmethod
    def for_daemon(
        cls,
        ctx: CephadmContext,
        ident: 'DaemonIdentity',
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
            identity=ident,
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
        if not self._cname and self.identity:
            return self.identity.container_name
        return self._cname.replace('.', '-')

    @cname.setter
    def cname(self, val: str) -> None:
        self._cname = val

    @property
    def old_cname(self) -> str:
        if not self._cname and self.identity:
            return self.identity.legacy_container_name
        return self._cname

    def run_cmd(self) -> List[str]:
        if not (self.envs and self.envs[0].startswith('NODE_NAME=')):
            self.envs.insert(0, 'NODE_NAME=%s' % get_hostname())
        return self.build_run_cmd()

    def rm_cmd(
        self, old_cname: bool = False, storage: bool = False
    ) -> List[str]:
        return self.build_rm_cmd(
            cname=self.old_cname if old_cname else self.cname,
            storage=storage,
        )

    def stop_cmd(
        self, old_cname: bool = False, timeout: Optional[int] = None
    ) -> List[str]:
        return self.build_stop_cmd(
            cname=self.old_cname if old_cname else self.cname,
            timeout=timeout,
        )

    def shell_cmd(self, cmd: List[str]) -> List[str]:
        cmd_args: List[str] = [
            str(self.ctx.container_engine.path),
            'run',
            '--rm',
            '--ipc=host',
        ]
        envs: List[str] = [
            '-e',
            'CONTAINER_IMAGE=%s' % self.image,
            '-e',
            'NODE_NAME=%s' % get_hostname(),
        ]
        vols: List[str] = []
        binds: List[str] = []

        if self.host_network:
            cmd_args.append('--net=host')
        if self.ctx.no_hosts:
            cmd_args.append('--no-hosts')
        if self.privileged:
            cmd_args.extend(
                [
                    '--privileged',
                    # let OSD etc read block devs that haven't been chowned
                    '--group-add=disk',
                ]
            )
        if self.init:
            cmd_args.append('--init')
        if self.envs:
            for env in self.envs:
                envs.extend(['-e', env])

        vols = sum(
            [
                ['-v', '%s:%s' % (host_dir, container_dir)]
                for host_dir, container_dir in self.volume_mounts.items()
            ],
            [],
        )
        binds = sum(
            [
                ['--mount', '{}'.format(','.join(bind))]
                for bind in self.bind_mounts
            ],
            [],
        )

        return (
            cmd_args
            + self.container_args
            + envs
            + vols
            + binds
            + [
                '--entrypoint',
                cmd[0],
                self.image,
            ]
            + cmd[1:]
        )

    def exec_cmd(self, cmd):
        # type: (List[str]) -> List[str]
        cname = get_running_container_name(self.ctx, self)
        if not cname:
            raise Error('unable to find container "{}"'.format(self.cname))
        return (
            [
                str(self.ctx.container_engine.path),
                'exec',
            ]
            + self.container_args
            + [
                self.cname,
            ]
            + cmd
        )

    def run(
        self,
        timeout=DEFAULT_TIMEOUT,
        verbosity=CallVerbosity.VERBOSE_ON_FAILURE,
    ):
        # type: (Optional[int], CallVerbosity) -> str
        out, _, _ = call_throws(
            self.ctx,
            self.run_cmd(),
            desc=self.entrypoint,
            timeout=timeout,
            verbosity=verbosity,
        )
        return out


class InitContainer(BasicContainer):
    @classmethod
    def from_primary_and_opts(
        cls,
        ctx: CephadmContext,
        primary: 'CephContainer',
        opts: Dict[str, Any],
        data_dir: str = '',
    ) -> 'InitContainer':
        if not opts:
            raise Error('no init container values provided')
        # volume mounts are specified relative to a dir in custom container
        # if we are going to inherit the dirs from the primary then we
        # just copy it. If not, we have to convert the relative paths
        # into absolute paths.
        assert primary.identity
        vmounts = opts.get('volume_mounts')
        if not vmounts:
            vmounts = primary.volume_mounts
        else:
            data_dir = data_dir or primary.identity.data_dir(ctx.data_dir)
            vmounts = {
                os.path.join(data_dir, src): dst
                for src, dst in vmounts.items()
            }
        return cls(
            ctx,
            identity=DaemonSubIdentity.from_parent(primary.identity, 'init'),
            image=opts.get('image', primary.image),
            entrypoint=opts.get('entrypoint', primary.entrypoint),
            # note: args is not inherited from primary container
            args=opts.get('entrypoint_args', []),
            volume_mounts=vmounts,
            envs=opts.get('envs', primary.envs),
            # note: privileged is not inherited from primary container
            # we really ought to minimize running stuff as privileged
            privileged=opts.get('privileged', False),
            init=False,
            ptrace=primary.ptrace,
            remove=False,
            memory_request=primary.memory_request,
            memory_limit=primary.memory_limit,
        )
        # Things we are currently not handling:
        #  container_args, bind_mounts, network, ipc

    def run_cmd(self) -> List[str]:
        return self.build_run_cmd()

    def rm_cmd(self, storage: bool = False) -> List[str]:
        return self.build_rm_cmd(storage=storage)

    def stop_cmd(self, timeout: Optional[int] = None) -> List[str]:
        return self.build_stop_cmd(timeout=timeout)


class SidecarContainer(BasicContainer):
    @classmethod
    def from_primary_and_values(
        cls,
        ctx: CephadmContext,
        primary: BasicContainer,
        sidecar_name: str,
        *,
        image: str = '',
        entrypoint: str = '',
        args: Optional[List[str]] = None,
        init: Optional[bool] = None,
    ) -> 'SidecarContainer':
        assert primary.identity
        identity = DaemonSubIdentity.from_parent(
            primary.identity, sidecar_name
        )
        ctr = cast(
            SidecarContainer, cls.from_container(primary, ident=identity)
        )
        ctr.remove = True
        if image:
            ctr.image = image
        if entrypoint:
            ctr.entrypoint = entrypoint
        if args:
            ctr.args = args
        if init is not None:
            ctr.init = init
        return ctr

    def build_engine_run_args(self) -> List[str]:
        assert isinstance(self.identity, DaemonSubIdentity)
        cmd_args = super().build_engine_run_args()
        if self._using_podman:
            # sidecar containers are always services, otherwise they
            # would not be sidecars
            cmd_args += self.ctx.container_engine.service_args(
                self.ctx, self.identity.sidecar_service_name
            )
        return cmd_args

    def run_cmd(self) -> List[str]:
        if not (self.envs and self.envs[0].startswith('NODE_NAME=')):
            self.envs.insert(0, 'NODE_NAME=%s' % get_hostname())
        return self.build_run_cmd()

    def rm_cmd(self, storage: bool = False) -> List[str]:
        return self.build_rm_cmd(storage=storage)

    def stop_cmd(self, timeout: Optional[int] = None) -> List[str]:
        return self.build_stop_cmd(timeout=timeout)


def is_container_running(ctx: CephadmContext, c: 'CephContainer') -> bool:
    if ctx.name.split('.', 1)[0] in ['agent', 'cephadm-exporter']:
        # these are non-containerized daemon types
        return False
    return bool(get_running_container_name(ctx, c))


def get_running_container_name(
    ctx: CephadmContext, c: 'CephContainer'
) -> Optional[str]:
    for name in [c.cname, c.old_cname]:
        out, err, ret = call(
            ctx,
            [
                ctx.container_engine.path,
                'container',
                'inspect',
                '--format',
                '{{.State.Status}}',
                name,
            ],
        )
        if out.strip() == 'running':
            return name
    return None


def extract_uid_gid(
    ctx: CephadmContext,
    img: str = '',
    file_path: Union[str, List[str]] = '/var/lib/ceph',
) -> Tuple[int, int]:
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
                ctx, image=img, entrypoint='stat', args=['-c', '%u %g', fp]
            ).run(verbosity=CallVerbosity.QUIET_UNLESS_ERROR)
            uid, gid = out.split(' ')
            return int(uid), int(gid)
        except RuntimeError as e:
            ex = (fp, e)
    if ex:
        raise Error(f'Failed to extract uid/gid for path {ex[0]}: {ex[1]}')

    raise RuntimeError('uid/gid not found')
