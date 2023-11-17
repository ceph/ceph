import logging
import os
import re

from typing import Dict, List, Optional, Tuple

from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer, SidecarContainer, extract_uid_gid
from ..context_getters import fetch_configs, get_config_and_keyring
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..constants import DEFAULT_IMAGE
from ..context import CephadmContext
from ..data_utils import dict_get, is_fsid
from ..deployment_utils import to_deployment_container
from ..exceptions import Error
from ..file_utils import makedirs, populate_files
from ..call_wrappers import call, CallVerbosity


logger = logging.getLogger()


@register_daemon_form
class CephIscsi(ContainerDaemonForm):
    """Defines a Ceph-Iscsi container"""

    daemon_type = 'iscsi'
    entrypoint = '/usr/bin/rbd-target-api'

    required_files = ['iscsi-gateway.cfg']

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        ctx: CephadmContext,
        ident: DaemonIdentity,
        config_json: Dict,
        image: str = DEFAULT_IMAGE,
    ):
        self.ctx = ctx
        self._identity = ident
        self.image = image

        # config-json options
        self.files = dict_get(config_json, 'files', {})

        # validate the supplied args
        self.validate()

    @classmethod
    def init(
        cls, ctx: CephadmContext, fsid: str, daemon_id: str
    ) -> 'CephIscsi':
        return cls.create(
            ctx, DaemonIdentity(fsid, cls.daemon_type, daemon_id)
        )

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'CephIscsi':
        return cls(ctx, ident, fetch_configs(ctx), ctx.image)

    @property
    def identity(self) -> DaemonIdentity:
        return self._identity

    @property
    def fsid(self) -> str:
        return self._identity.fsid

    @property
    def daemon_id(self) -> str:
        return self._identity.daemon_id

    @staticmethod
    def _get_container_mounts(data_dir, log_dir):
        # type: (str, str) -> Dict[str, str]
        mounts = dict()
        mounts[os.path.join(data_dir, 'config')] = '/etc/ceph/ceph.conf:z'
        mounts[os.path.join(data_dir, 'keyring')] = '/etc/ceph/keyring:z'
        mounts[
            os.path.join(data_dir, 'iscsi-gateway.cfg')
        ] = '/etc/ceph/iscsi-gateway.cfg:z'
        mounts[os.path.join(data_dir, 'configfs')] = '/sys/kernel/config'
        mounts[
            os.path.join(data_dir, 'tcmu-runner-entrypoint.sh')
        ] = '/usr/local/scripts/tcmu-runner-entrypoint.sh'
        mounts[log_dir] = '/var/log:z'
        mounts['/dev'] = '/dev'
        return mounts

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        # Removes ending ".tcmu" from data_dir a tcmu-runner uses the same
        # data_dir as rbd-runner-api
        if data_dir.endswith('.tcmu'):
            data_dir = re.sub(r'\.tcmu$', '', data_dir)
        log_dir = os.path.join(ctx.log_dir, self.identity.fsid)
        mounts.update(CephIscsi._get_container_mounts(data_dir, log_dir))

    def customize_container_binds(
        self, ctx: CephadmContext, binds: List[List[str]]
    ) -> None:
        lib_modules = [
            'type=bind',
            'source=/lib/modules',
            'destination=/lib/modules',
            'ro=true',
        ]
        binds.append(lib_modules)

    @staticmethod
    def get_version(ctx, container_id):
        # type: (CephadmContext, str) -> Optional[str]
        version = None
        out, err, code = call(
            ctx,
            [
                ctx.container_engine.path,
                'exec',
                container_id,
                '/usr/bin/python3',
                '-c',
                "import pkg_resources; print(pkg_resources.require('ceph_iscsi')[0].version)",
            ],
            verbosity=CallVerbosity.QUIET,
        )
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
                    raise Error(
                        'required file missing from config-json: %s' % fname
                    )

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
        self.files[
            'tcmu-runner-entrypoint.sh'
        ] = self.tcmu_runner_entrypoint_script()

        # populate files from the config-json
        populate_files(data_dir, self.files, uid, gid)

        # we want the tcmu runner entrypoint script to be executable
        # populate_files will give it 0o600 by default
        os.chmod(os.path.join(data_dir, 'tcmu-runner-entrypoint.sh'), 0o700)

    @staticmethod
    def configfs_mount_umount(data_dir: str, mount: bool = True) -> str:
        mount_path = os.path.join(data_dir, 'configfs')
        if mount:
            cmd = (
                'if ! grep -qs {0} /proc/mounts; then '
                'mount -t configfs none {0}; fi'.format(mount_path)
            )
        else:
            cmd = (
                'if grep -qs {0} /proc/mounts; then '
                'umount {0}; fi'.format(mount_path)
            )
        return cmd

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

    def container(self, ctx: CephadmContext) -> CephContainer:
        # So the container can modprobe iscsi_target_mod and have write perms
        # to configfs we need to make this a privileged container.
        ctr = daemon_to_container(ctx, self, privileged=True)
        return to_deployment_container(ctx, ctr)

    def config_and_keyring(
        self, ctx: CephadmContext
    ) -> Tuple[Optional[str], Optional[str]]:
        return get_config_and_keyring(ctx)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return extract_uid_gid(ctx)

    def default_entrypoint(self) -> str:
        return self.entrypoint

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.append(ctx.container_engine.unlimited_pids_option)

    def sidecar_containers(
        self, ctx: CephadmContext
    ) -> List[SidecarContainer]:
        tcmu_sidecar = SidecarContainer.from_primary_and_values(
            ctx,
            self.container(ctx),
            'tcmu',
            # TODO: Eventually we don't want to run tcmu-runner through this
            # script.  This is intended to be a workaround backported to older
            # releases and should eventually be removed in at least squid
            # onward
            entrypoint='/usr/local/scripts/tcmu-runner-entrypoint.sh',
        )
        return [tcmu_sidecar]
