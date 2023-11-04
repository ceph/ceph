# container_deamon_form.py - base class for container based daemon forms

import abc

from typing import List, Tuple, Optional, Dict

from .container_types import CephContainer, InitContainer
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

    def customize_container_args(self, args: List[str]) -> None:
        """Given a list of container arguments this function can update,
        delete, or otherwise mutate the arguments that the container engine
        will use.
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
