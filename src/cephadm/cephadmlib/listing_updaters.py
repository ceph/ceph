# Additional types to help with container & daemon listing

from typing import Any, Dict, List, Optional

import json
import logging
import os

from .call_wrappers import call, CallVerbosity
from .container_engines import (
    normalize_container_id,
    parsed_container_cpu_perc,
    parsed_container_mem_usage,
)
from .container_types import get_container_stats
from .context import CephadmContext
from .daemon_identity import DaemonIdentity
from .daemons import (
    CephIscsi,
    CephNvmeof,
    CustomContainer,
    Monitoring,
    NFSGanesha,
    SMB,
    SNMPGateway,
    MgmtGateway,
    OAuth2Proxy,
)
from .daemons.ceph import ceph_daemons
from .data_utils import normalize_image_digest, try_convert_datetime
from .file_utils import get_file_timestamp
from .listing import DaemonStatusUpdater
from .systemd import check_unit


logger = logging.getLogger()


class CoreStatusUpdater(DaemonStatusUpdater):
    def __init__(self, keep_container_info: str = '') -> None:
        # set keep_container_info to a custom key that will be used to cache
        # the ContainerInfo object in the status dict.
        self.keep_container_info = keep_container_info

    def update(
        self,
        val: Dict[str, Any],
        ctx: CephadmContext,
        identity: DaemonIdentity,
        data_dir: str,
    ) -> None:
        enabled, state, _ = check_unit(ctx, identity.unit_name)
        val['enabled'] = enabled
        val['state'] = state

        container_id = image_name = image_id = version = None
        start_stamp = None
        daemon_dir = os.path.join(
            data_dir, identity.fsid, identity.daemon_name
        )
        cinfo = get_container_stats(ctx, identity)
        if self.keep_container_info:
            val[self.keep_container_info] = cinfo
        if cinfo:
            container_id = cinfo.container_id
            image_name = cinfo.image_name
            image_id = cinfo.image_id
            version = cinfo.version
            image_id = normalize_container_id(image_id)
            start_stamp = try_convert_datetime(cinfo.start)
        else:
            vfile = os.path.join(daemon_dir, 'unit.image')
            try:
                with open(vfile, 'r') as f:
                    image_name = f.read().strip() or None
            except IOError:
                pass

        # unit.meta?
        mfile = os.path.join(daemon_dir, 'unit.meta')
        try:
            with open(mfile, 'r') as f:
                meta = json.loads(f.read())
                val.update(meta)
        except IOError:
            pass

        val['container_id'] = container_id
        val['container_image_name'] = image_name
        val['container_image_id'] = image_id
        val['version'] = version
        val['started'] = start_stamp
        val['created'] = get_file_timestamp(
            os.path.join(daemon_dir, 'unit.created')
        )
        val['deployed'] = get_file_timestamp(
            os.path.join(daemon_dir, 'unit.image')
        )
        val['configured'] = get_file_timestamp(
            os.path.join(daemon_dir, 'unit.configured')
        )

    def legacy_update(
        self,
        val: Dict[str, Any],
        ctx: CephadmContext,
        fsid: str,
        daemon_type: str,
        name: str,
        data_dir: str,
    ) -> None:
        cache = getattr(self, '_cache', {})
        setattr(self, '_cache', cache)
        legacy_unit_name = val['name']
        (val['enabled'], val['state'], _) = check_unit(ctx, legacy_unit_name)
        if not cache.get('host_version'):
            try:
                out, err, code = call(
                    ctx,
                    ['ceph', '-v'],
                    verbosity=CallVerbosity.QUIET,
                )
                if not code and out.startswith('ceph version '):
                    cache['host_version'] = out.split(' ')[2]
            except Exception:
                pass
        val['host_version'] = cache.get('host_version')


class DigestsStatusUpdater(DaemonStatusUpdater):
    def __init__(self) -> None:
        self.seen_digests: Dict[str, List[str]] = {}

    def update(
        self,
        val: Dict[str, Any],
        ctx: CephadmContext,
        identity: DaemonIdentity,
        data_dir: str,
    ) -> None:
        # collect digests for this image id
        assert 'container_image_digests' not in val
        image_id = val.get('container_image_id', None)
        if not image_id:
            val['container_image_digests'] = None
            return  # container info missing or no longer running?
        container_path = ctx.container_engine.path
        image_digests = self.seen_digests.get(image_id)
        if not image_digests:
            out, err, code = call(
                ctx,
                [
                    container_path,
                    'image',
                    'inspect',
                    image_id,
                    '--format',
                    '{{.RepoDigests}}',
                ],
                verbosity=CallVerbosity.QUIET,
            )
            if not code:
                image_digests = list(
                    set(
                        map(
                            normalize_image_digest,
                            out.strip()[1:-1].split(' '),
                        )
                    )
                )
                self.seen_digests[image_id] = image_digests
        val['container_image_digests'] = image_digests


class VersionStatusUpdater(DaemonStatusUpdater):
    def __init__(self) -> None:
        self.seen_versions: Dict[str, Optional[str]] = {}

    def update(
        self,
        val: Dict[str, Any],
        ctx: CephadmContext,
        identity: DaemonIdentity,
        data_dir: str,
    ) -> None:
        # collect digests for this image id
        container_path = ctx.container_engine.path
        image_id = val.get('container_image_id', None)
        version = val.get('version', None)
        daemon_type = identity.daemon_type
        container_id = val['container_id']
        if not image_id and not version:
            return  # container info missing or no longer running?
        # identify software version inside the container (if we can)
        if not version or '.' not in version:
            version = self.seen_versions.get(image_id, None)
        if daemon_type == NFSGanesha.daemon_type:
            version = NFSGanesha.get_version(ctx, container_id)
        if daemon_type == CephIscsi.daemon_type:
            version = CephIscsi.get_version(ctx, container_id)
        if daemon_type == CephNvmeof.daemon_type:
            version = CephNvmeof.get_version(ctx, container_id)
        if daemon_type == SMB.daemon_type:
            version = SMB.get_version(ctx, container_id)
        elif not version:
            if daemon_type in ceph_daemons():
                out, err, code = call(
                    ctx,
                    [
                        container_path,
                        'exec',
                        container_id,
                        'ceph',
                        '-v',
                    ],
                    verbosity=CallVerbosity.QUIET,
                )
                if not code and out.startswith('ceph version '):
                    version = out.split(' ')[2]
                    self.seen_versions[image_id] = version
            elif daemon_type == 'grafana':
                out, err, code = call(
                    ctx,
                    [
                        container_path,
                        'exec',
                        container_id,
                        'grafana',
                        'server',
                        '-v',
                    ],
                    verbosity=CallVerbosity.QUIET,
                )
                if not code and out.startswith('Version '):
                    version = out.split(' ')[1]
                    self.seen_versions[image_id] = version
            elif daemon_type in [
                'prometheus',
                'alertmanager',
                'node-exporter',
                'loki',
                'promtail',
            ]:
                version = Monitoring.get_version(
                    ctx, container_id, daemon_type
                )
                self.seen_versions[image_id] = version
            elif daemon_type == 'haproxy':
                out, err, code = call(
                    ctx,
                    [
                        container_path,
                        'exec',
                        container_id,
                        'haproxy',
                        '-v',
                    ],
                    verbosity=CallVerbosity.QUIET,
                )
                if (
                    not code
                    and out.startswith('HA-Proxy version ')
                    or out.startswith('HAProxy version ')
                ):
                    version = out.split(' ')[2]
                    self.seen_versions[image_id] = version
            elif daemon_type == 'keepalived':
                out, err, code = call(
                    ctx,
                    [
                        container_path,
                        'exec',
                        container_id,
                        'keepalived',
                        '--version',
                    ],
                    verbosity=CallVerbosity.QUIET,
                )
                if not code and err.startswith('Keepalived '):
                    version = err.split(' ')[1]
                    if version[0] == 'v':
                        version = version[1:]
                    self.seen_versions[image_id] = version
            elif daemon_type == CustomContainer.daemon_type:
                # Because a custom container can contain
                # everything, we do not know which command
                # to execute to get the version.
                pass
            elif daemon_type == SNMPGateway.daemon_type:
                version = SNMPGateway.get_version(
                    ctx, identity.fsid, identity.daemon_id
                )
                self.seen_versions[image_id] = version
            elif daemon_type == MgmtGateway.daemon_type:
                version = MgmtGateway.get_version(ctx, container_id)
                self.seen_versions[image_id] = version
            elif daemon_type == OAuth2Proxy.daemon_type:
                version = OAuth2Proxy.get_version(ctx, container_id)
                self.seen_versions[image_id] = version
            else:
                logger.warning(
                    'version for unknown daemon type %s' % daemon_type
                )
        val['version'] = version


class MemUsageStatusUpdater(DaemonStatusUpdater):
    def __init__(self) -> None:
        self._loaded = False
        self.seen_memusage_cid_len: int = 0
        self.seen_memusage: Dict[str, int] = {}

    def _load(self, ctx: CephadmContext) -> None:
        if self._loaded:
            return
        length, memusage = parsed_container_mem_usage(ctx)
        self.seen_memusage_cid_len = length
        self.seen_memusage = memusage
        self._loaded = True

    def update(
        self,
        val: Dict[str, Any],
        ctx: CephadmContext,
        identity: DaemonIdentity,
        data_dir: str,
    ) -> None:
        container_id = val.get('container_id', None)
        if container_id:
            self._load(ctx)
            val['memory_usage'] = self.seen_memusage.get(
                container_id[0 : self.seen_memusage_cid_len]
            )


class CPUUsageStatusUpdater(DaemonStatusUpdater):
    def __init__(self) -> None:
        self._loaded = False
        self.seen_cpuperc_cid_len: int = 0
        self.seen_cpuperc: Dict[str, str] = {}

    def _load(self, ctx: CephadmContext) -> None:
        if self._loaded:
            return
        length, cpuperc = parsed_container_cpu_perc(ctx)
        self.seen_cpuperc_cid_len = length
        self.seen_cpuperc = cpuperc
        self._loaded = True

    def update(
        self,
        val: Dict[str, Any],
        ctx: CephadmContext,
        identity: DaemonIdentity,
        data_dir: str,
    ) -> None:
        container_id = val.get('container_id', None)
        if container_id:
            self._load(ctx)
            val['cpu_percentage'] = self.seen_cpuperc.get(
                container_id[0 : self.seen_cpuperc_cid_len]
            )
