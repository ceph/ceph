# systemd_unit.py - creating/managing systemd unit files

import contextlib
import os
import pathlib

from typing import IO, List, Optional, Union

from . import templating
from .call_wrappers import call_throws
from .container_engines import Docker, Podman
from .context import CephadmContext
from .daemon_identity import DaemonIdentity, DaemonSubIdentity
from .file_utils import write_new
from .logging import write_cluster_logrotate_config


_DROP_IN_FILENAME = '99-cephadm.conf'


class PathInfo:
    """Utility class to map basic service identities, to the paths used by
    their corresponding systemd unit files.
    """

    def __init__(
        self,
        unit_dir: Union[str, pathlib.Path],
        identity: DaemonIdentity,
        sidecar_ids: Optional[List[DaemonSubIdentity]] = None,
    ) -> None:
        self.identity = identity
        self.sidecar_ids = sidecar_ids or []

        unit_dir = pathlib.Path(unit_dir)
        self.default_unit_file = unit_dir / f'ceph-{identity.fsid}@.service'
        self.init_ctr_unit_file = unit_dir / identity.init_service_name
        self.sidecar_unit_files = {
            si: unit_dir / si.sidecar_service_name for si in self.sidecar_ids
        }
        dname = f'{identity.service_name}.d'
        self.drop_in_file = unit_dir / dname / _DROP_IN_FILENAME


def _write_drop_in(
    dest: IO,
    ctx: CephadmContext,
    identity: DaemonIdentity,
    enable_init_containers: bool,
    sidecar_ids: List[DaemonSubIdentity],
) -> None:
    templating.render_to_file(
        dest,
        ctx,
        templating.Templates.dropin_service,
        identity=identity,
        enable_init_containers=enable_init_containers,
        sidecar_ids=sidecar_ids,
    )


def _write_init_containers_unit_file(
    dest: IO, ctx: CephadmContext, identity: DaemonIdentity
) -> None:
    has_docker_engine = isinstance(ctx.container_engine, Docker)
    has_podman_engine = isinstance(ctx.container_engine, Podman)
    templating.render_to_file(
        dest,
        ctx,
        templating.Templates.init_ctr_service,
        identity=identity,
        has_docker_engine=has_docker_engine,
        has_podman_engine=has_podman_engine,
        has_podman_split_version=(
            has_podman_engine and ctx.container_engine.supports_split_cgroups
        ),
    )


def _write_sidecar_unit_file(
    dest: IO, ctx: CephadmContext, primary: DaemonIdentity, sidecar: DaemonSubIdentity
) -> None:
    has_docker_engine = isinstance(ctx.container_engine, Docker)
    has_podman_engine = isinstance(ctx.container_engine, Podman)
    templating.render_to_file(
        dest,
        ctx,
        templating.Templates.sidecar_service,
        primary=primary,
        sidecar=sidecar,
        sidecar_script=sidecar.sidecar_script(ctx.data_dir),
        has_docker_engine=has_docker_engine,
        has_podman_engine=has_podman_engine,
        has_podman_split_version=(
            has_podman_engine and ctx.container_engine.supports_split_cgroups
        ),
    )


def _install_extended_systemd_services(
    ctx: CephadmContext,
    pinfo: PathInfo,
    identity: DaemonIdentity,
    enable_init_containers: bool = False,
) -> None:
    """Install the systemd unit files needed for more complex services
    that have init containers and/or sidecars.
    """
    with contextlib.ExitStack() as estack:
        # install the unit file to handle running init containers
        if enable_init_containers:
            icfh = estack.enter_context(
                write_new(pinfo.init_ctr_unit_file, perms=None)
            )
            _write_init_containers_unit_file(icfh, ctx, identity)

        # install the unit files to handle running sidecars
        sids = []
        for si, sup in pinfo.sidecar_unit_files.items():
            sufh = estack.enter_context(write_new(sup, perms=None))
            _write_sidecar_unit_file(sufh, ctx, identity, si)
            sids.append(si)

        # create a drop-in to create a relationship between the primary
        # service and the init- and sidecar-based services.
        if enable_init_containers or sids:
            pinfo.drop_in_file.parent.mkdir(parents=True, exist_ok=True)
            difh = estack.enter_context(
                write_new(pinfo.drop_in_file, perms=None)
            )
            _write_drop_in(
                difh, ctx, identity, enable_init_containers, sids
            )


def _get_unit_file(ctx: CephadmContext, fsid: str) -> str:
    has_docker_engine = isinstance(ctx.container_engine, Docker)
    has_podman_engine = isinstance(ctx.container_engine, Podman)
    has_podman_split_version = (
        has_podman_engine and ctx.container_engine.supports_split_cgroups
    )
    return templating.render(
        ctx,
        templating.Templates.ceph_service,
        fsid=fsid,
        has_docker_engine=has_docker_engine,
        has_podman_engine=has_podman_engine,
        has_podman_split_version=has_podman_split_version,
    )


def _install_base_units(ctx: CephadmContext, fsid: str) -> None:
    """
    Set up ceph.target and ceph-$fsid.target units.
    """
    # global unit
    existed = os.path.exists(ctx.unit_dir + '/ceph.target')
    with write_new(ctx.unit_dir + '/ceph.target', perms=None) as f:
        f.write(
            '[Unit]\n'
            'Description=All Ceph clusters and services\n'
            '\n'
            '[Install]\n'
            'WantedBy=multi-user.target\n'
        )
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
            'WantedBy=multi-user.target ceph.target\n'.format(fsid=fsid)
        )
    if not existed:
        call_throws(ctx, ['systemctl', 'enable', 'ceph-%s.target' % fsid])
        call_throws(ctx, ['systemctl', 'start', 'ceph-%s.target' % fsid])

    # don't overwrite file in order to allow users to manipulate it
    if os.path.exists(ctx.logrotate_dir + f'/ceph-{fsid}'):
        return

    write_cluster_logrotate_config(ctx, fsid)


def update_files(
    ctx: CephadmContext,
    ident: DaemonIdentity,
    *,
    init_container_ids: Optional[List[DaemonSubIdentity]] = None,
    sidecar_ids: Optional[List[DaemonSubIdentity]] = None,
) -> None:
    _install_base_units(ctx, ident.fsid)
    unit = _get_unit_file(ctx, ident.fsid)
    pathinfo = PathInfo(ctx.unit_dir, ident, sidecar_ids=sidecar_ids)
    with write_new(pathinfo.default_unit_file, perms=None) as f:
        f.write(unit)
    _install_extended_systemd_services(
        ctx, pathinfo, ident, bool(init_container_ids)
    )


def sidecars_from_dropin(
    pathinfo: PathInfo, missing_ok: bool = False
) -> PathInfo:
    """Read the list of sidecars for a service from the service's drop in file."""
    # This is useful in the cases where the sidecars would be determined from
    # input data (deployment) but we lack the original deployment data (rm
    # daemon).
    sidecars = []
    try:
        with open(pathinfo.drop_in_file) as fh:
            lines = fh.readlines()
    except FileNotFoundError:
        if missing_ok:
            return pathinfo
        raise
    for line in lines:
        if not line.startswith('Wants='):
            continue
        for item in line[6:].strip().split():
            si, category = DaemonSubIdentity.from_service_name(item)
            if category == 'sidecar':
                sidecars.append(si)
    return PathInfo(
        pathinfo.default_unit_file.parent, pathinfo.identity, sidecars
    )
