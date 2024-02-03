# container_deamon_form.py - base class for container based daemon forms

import abc

from typing import List, Tuple, Optional, Dict

from .container_engines import Podman
from .container_types import CephContainer, InitContainer, SidecarContainer
from .context import CephadmContext
from .daemon_form import DaemonForm
from .deploy import DeploymentType
from .net_utils import EndPoint


class ContainerDaemonForm(DaemonForm):
    """A ContainerDaemonForm is a variety of DaemonForm that runs a
    single primary daemon process under as a container.
    It requires that the `container` method be implemented by subclasses.
    A number of other optional methods may also be overridden.
    """

    @abc.abstractmethod
    def container(self, ctx: CephadmContext) -> CephContainer:
        """Return the CephContainer instance that will be used to build and run
        the daemon.
        """
        raise NotImplementedError()  # pragma: no cover

    @abc.abstractmethod
    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        """Return a (uid, gid) tuple indicating what UID and GID the daemon is
        expected to run as. This function is permitted to take complex actions
        such as running a container to get the needed information.
        """
        raise NotImplementedError()  # pragma: no cover

    def init_containers(self, ctx: CephadmContext) -> List[InitContainer]:
        """Returns a list of init containers to execute prior to the primary
        container running. By default, returns an empty list.
        """
        return []

    def sidecar_containers(self, ctx: CephadmContext) -> List[SidecarContainer]:
        """Returns a list of sidecar containers that should be executed along
        with the primary service container.
        """
        return []

    def customize_container_binds(
        self, ctx: CephadmContext, binds: List[List[str]]
    ) -> None:
        """Given a list of container binds this function can update, delete,
        or otherwise mutate the binds that the container will use.
        """
        pass

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        """Given a list of container mounts this function can update, delete,
        or otherwise mutate the mounts that the container will use.
        """
        pass

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        """Given a list of container arguments this function can update,
        delete, or otherwise mutate the arguments that the container engine
        will use.
        """
        pass

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        """Given a list of arguments for the containerized process, this
        function can update, delete, or otherwise mutate the arguments that the
        process will use.
        """
        pass

    def customize_container_envs(
        self, ctx: CephadmContext, envs: List[str]
    ) -> None:
        """Given a list of environment vars this function can update, delete,
        or otherwise mutate the environment variables that are passed by the
        container engine to the processes it executes.
        """
        pass

    def customize_container_endpoints(
        self, endpoints: List[EndPoint], deployment_type: DeploymentType
    ) -> None:
        """Given a list of entrypoints this function can update, delete,
        or otherwise mutate the entrypoints that the container will use.
        """
        pass

    def config_and_keyring(
        self, ctx: CephadmContext
    ) -> Tuple[Optional[str], Optional[str]]:
        """Return a tuple of strings containing the ceph confguration
        and keyring for the daemon. Returns (None, None) by default.
        """
        return None, None

    @property
    def osd_fsid(self) -> Optional[str]:
        """Return the OSD FSID or None. Pretty specific to OSDs. You are not
        expected to understand this.
        """
        return None

    def default_entrypoint(self) -> str:
        """Return the default entrypoint value when running a deamon process
        in a container.
        """
        return ''


def daemon_to_container(
    ctx: CephadmContext,
    daemon: ContainerDaemonForm,
    *,
    privileged: bool = False,
    ptrace: bool = False,
    host_network: bool = True,
    entrypoint: Optional[str] = None,
    container_args: Optional[List[str]] = None,
    container_mounts: Optional[Dict[str, str]] = None,
    container_binds: Optional[List[List[str]]] = None,
    envs: Optional[List[str]] = None,
    args: Optional[List[str]] = None,
    auto_podman_args: bool = True,
    auto_podman_mounts: bool = True,
) -> CephContainer:
    """daemon_to_container is a utility function that serves to create
    CephContainer instances from a container daemon form's customize and
    entrypoint methods.
    Most of the parameters (like mounts, container_args, etc) can be passed in
    to "pre customize" the values.
    The auto_podman_args argument enables adding default arguments expected on
    all podman daemons (true by default).
    The auto_podman_mounts argument enables adding mounts expected on all
    daemons running on podman (true by default).
    """
    container_args = container_args if container_args else []
    container_mounts = container_mounts if container_mounts else {}
    container_binds = container_binds if container_binds else []
    envs = envs if envs else []
    args = args if args else []

    if entrypoint is None:
        entrypoint = daemon.default_entrypoint()
    daemon.customize_container_args(ctx, container_args)
    daemon.customize_container_mounts(ctx, container_mounts)
    daemon.customize_container_binds(ctx, container_binds)
    daemon.customize_container_envs(ctx, envs)
    daemon.customize_process_args(ctx, args)

    _is_podman = isinstance(ctx.container_engine, Podman)
    if auto_podman_mounts and _is_podman:
        ctx.container_engine.update_mounts(ctx, container_mounts)
    if auto_podman_args and _is_podman:
        container_args.extend(
            ctx.container_engine.service_args(ctx, daemon.identity.service_name)
        )

    return CephContainer.for_daemon(
        ctx,
        ident=daemon.identity,
        entrypoint=entrypoint,
        args=args,
        container_args=container_args,
        volume_mounts=container_mounts,
        bind_mounts=container_binds,
        envs=envs,
        privileged=privileged,
        ptrace=ptrace,
        host_network=host_network,
    )
