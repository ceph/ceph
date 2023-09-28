# systemd_unit.py - creating/managing systemd unit files

import os

from . import templating
from .call_wrappers import call_throws
from .container_engines import Docker, Podman
from .context import CephadmContext
from .file_utils import write_new
from .logging import write_cluster_logrotate_config


def get_unit_file(ctx: CephadmContext, fsid: str) -> str:
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

    write_cluster_logrotate_config(ctx, fsid)
