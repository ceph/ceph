import os
import logging
import json
import datetime
from ..constants import DATEFMT

from ..container_types import CephContainer
from cephadmlib.data_utils import (
    get_legacy_config_fsid,
    is_fsid,
)

from .iscsi import CephIscsi
from .nvmeof import CephNvmeof
from .custom import CustomContainer
from .monitoring import Monitoring
from .nfs import NFSGanesha
from .smb import SMB
from .snmp import SNMPGateway
from .mgmt_gateway import MgmtGateway
from .oauth2_proxy import OAuth2Proxy

from ..daemon_identity import DaemonIdentity
from cephadmlib.context import CephadmContext
from cephadmlib.systemd import check_unit
from cephadmlib.data_utils import (
    normalize_image_digest,
    try_convert_datetime,
    with_units_to_int,
)

from typing import Any, Dict, List, Optional, Tuple, Union
from cephadmlib.call_wrappers import CallVerbosity, call

logger = logging.getLogger()

###########################################


class ContainerInfo:
    def __init__(
        self,
        container_id: str,
        image_name: str,
        image_id: str,
        start: str,
        version: str,
    ) -> None:
        self.container_id = container_id
        self.image_name = image_name
        self.image_id = image_id
        self.start = start
        self.version = version

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ContainerInfo):
            return NotImplemented
        return (
            self.container_id == other.container_id
            and self.image_name == other.image_name
            and self.image_id == other.image_id
            and self.start == other.start
            and self.version == other.version
        )


#############################################


def list_daemons(
    ctx: CephadmContext,
    detail: bool = True,
    legacy_dir: Optional[str] = None,
    daemon_name: Optional[str] = None,
) -> List[Dict[str, str]]:
    host_version: Optional[str] = None
    ls = []
    container_path = ctx.container_engine.path

    data_dir = ctx.data_dir
    if legacy_dir is not None:
        data_dir = os.path.abspath(legacy_dir + data_dir)

    # keep track of ceph versions we see
    seen_versions = {}  # type: Dict[str, Optional[str]]

    # keep track of image digests
    seen_digests = {}  # type: Dict[str, List[str]]

    # keep track of memory and cpu usage we've seen
    seen_memusage = {}  # type: Dict[str, int]
    seen_cpuperc = {}  # type: Dict[str, str]
    out, err, code = call(
        ctx,
        [
            container_path,
            'stats',
            '--format',
            '{{.ID}},{{.MemUsage}}',
            '--no-stream',
        ],
        verbosity=CallVerbosity.QUIET,
    )
    seen_memusage_cid_len, seen_memusage = _parse_mem_usage(code, out)

    out, err, code = call(
        ctx,
        [
            container_path,
            'stats',
            '--format',
            '{{.ID}},{{.CPUPerc}}',
            '--no-stream',
        ],
        verbosity=CallVerbosity.QUIET,
    )
    seen_cpuperc_cid_len, seen_cpuperc = _parse_cpu_perc(code, out)

    # /var/lib/ceph
    if os.path.exists(data_dir):
        for i in os.listdir(data_dir):
            if i in ['mon', 'osd', 'mds', 'mgr', 'rgw']:
                daemon_type = i
                for j in os.listdir(os.path.join(data_dir, i)):
                    if '-' not in j:
                        continue
                    (cluster, daemon_id) = j.split('-', 1)
                    fsid = get_legacy_daemon_fsid(
                        ctx,
                        cluster,
                        daemon_type,
                        daemon_id,
                        legacy_dir=legacy_dir,
                    )
                    legacy_unit_name = 'ceph-%s@%s' % (daemon_type, daemon_id)
                    val: Dict[str, Any] = {
                        'style': 'legacy',
                        'name': '%s.%s' % (daemon_type, daemon_id),
                        'fsid': fsid if fsid is not None else 'unknown',
                        'systemd_unit': legacy_unit_name,
                    }
                    if detail:
                        (val['enabled'], val['state'], _) = check_unit(
                            ctx, legacy_unit_name
                        )
                        if not host_version:
                            try:
                                out, err, code = call(
                                    ctx,
                                    ['ceph', '-v'],
                                    verbosity=CallVerbosity.QUIET,
                                )
                                if not code and out.startswith(
                                    'ceph version '
                                ):
                                    host_version = out.split(' ')[2]
                            except Exception:
                                pass
                        val['host_version'] = host_version
                    ls.append(val)
            elif is_fsid(i):
                fsid = str(i)  # convince mypy that fsid is a str here
                for j in os.listdir(os.path.join(data_dir, i)):
                    if '.' in j and os.path.isdir(
                        os.path.join(data_dir, fsid, j)
                    ):
                        name = j
                        if daemon_name and name != daemon_name:
                            continue
                        (daemon_type, daemon_id) = j.split('.', 1)
                        unit_name = get_unit_name(
                            fsid, daemon_type, daemon_id
                        )
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
                        (val['enabled'], val['state'], _) = check_unit(
                            ctx, unit_name
                        )
                        container_id = None
                        image_name = None
                        image_id = None
                        image_digests = None
                        version = None
                        start_stamp = None

                        out, err, code = get_container_stats(
                            ctx, container_path, fsid, daemon_type, daemon_id
                        )
                        if not code:
                            (
                                container_id,
                                image_name,
                                image_id,
                                start,
                                version,
                            ) = out.strip().split(',')
                            image_id = normalize_container_id(image_id)
                            daemon_type = name.split('.', 1)[0]
                            start_stamp = try_convert_datetime(start)

                            # collect digests for this image id
                            image_digests = seen_digests.get(image_id)
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
                                    seen_digests[image_id] = image_digests

                            # identify software version inside the container (if we can)
                            if not version or '.' not in version:
                                version = seen_versions.get(image_id, None)
                            if daemon_type == NFSGanesha.daemon_type:
                                version = NFSGanesha.get_version(
                                    ctx, container_id
                                )
                            if daemon_type == CephIscsi.daemon_type:
                                version = CephIscsi.get_version(
                                    ctx, container_id
                                )
                            if daemon_type == CephNvmeof.daemon_type:
                                version = CephNvmeof.get_version(
                                    ctx, container_id
                                )
                            if daemon_type == SMB.daemon_type:
                                version = SMB.get_version(ctx, container_id)
                            elif not version:
                                from .ceph import ceph_daemons

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
                                    if not code and out.startswith(
                                        'ceph version '
                                    ):
                                        version = out.split(' ')[2]
                                        seen_versions[image_id] = version
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
                                    if not code and out.startswith(
                                        'Version '
                                    ):
                                        version = out.split(' ')[1]
                                        seen_versions[image_id] = version
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
                                    seen_versions[image_id] = version
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
                                        and out.startswith(
                                            'HA-Proxy version '
                                        )
                                        or out.startswith('HAProxy version ')
                                    ):
                                        version = out.split(' ')[2]
                                        seen_versions[image_id] = version
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
                                    if not code and err.startswith(
                                        'Keepalived '
                                    ):
                                        version = err.split(' ')[1]
                                        if version[0] == 'v':
                                            version = version[1:]
                                        seen_versions[image_id] = version
                                elif (
                                    daemon_type == CustomContainer.daemon_type
                                ):
                                    # Because a custom container can contain
                                    # everything, we do not know which command
                                    # to execute to get the version.
                                    pass
                                elif daemon_type == SNMPGateway.daemon_type:
                                    version = SNMPGateway.get_version(
                                        ctx, fsid, daemon_id
                                    )
                                    seen_versions[image_id] = version
                                elif daemon_type == MgmtGateway.daemon_type:
                                    version = MgmtGateway.get_version(
                                        ctx, container_id
                                    )
                                    seen_versions[image_id] = version
                                elif daemon_type == OAuth2Proxy.daemon_type:
                                    version = OAuth2Proxy.get_version(
                                        ctx, container_id
                                    )
                                    seen_versions[image_id] = version
                                else:
                                    logger.warning(
                                        'version for unknown daemon type %s'
                                        % daemon_type
                                    )
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
                            val['memory_usage'] = seen_memusage.get(
                                container_id[0:seen_memusage_cid_len]
                            )
                            val['cpu_percentage'] = seen_cpuperc.get(
                                container_id[0:seen_cpuperc_cid_len]
                            )
                        val['version'] = version
                        val['started'] = start_stamp
                        val['created'] = get_file_timestamp(
                            os.path.join(data_dir, fsid, j, 'unit.created')
                        )
                        val['deployed'] = get_file_timestamp(
                            os.path.join(data_dir, fsid, j, 'unit.image')
                        )
                        val['configured'] = get_file_timestamp(
                            os.path.join(data_dir, fsid, j, 'unit.configured')
                        )
                    ls.append(val)

    return ls


def _get_matching_daemons_by_name(
    ctx: CephadmContext, daemon_filter: str
) -> List[Dict[str, str]]:
    # NOTE: we are not passing detail=False to this list_daemons call
    # as we want the container_image name in the case where we are
    # doing this by name and this is skipped when detail=False
    matching_daemons = list_daemons(ctx, daemon_name=daemon_filter)
    if len(matching_daemons) > 1:
        logger.warning(
            f'Found multiple daemons sharing same name: {daemon_filter}'
        )
        # Take the first daemon we find that is actually running, or just the
        # first in the list if none are running
        matched_daemon = None
        for d in matching_daemons:
            if 'state' in d and d['state'] == 'running':
                matched_daemon = d
                break
        if not matched_daemon:
            matched_daemon = matching_daemons[0]
        matching_daemons = [matched_daemon]
    return matching_daemons


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
                logger.info(
                    'unable to parse memory usage line\n>{}'.format(line)
                )
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
                logger.info(
                    'unable to parse cpu percentage line\n>{}'.format(line)
                )
                pass
    return seen_cpuperc_cid_len, seen_cpuperc


def get_legacy_daemon_fsid(
    ctx, cluster, daemon_type, daemon_id, legacy_dir=None
):
    # type: (CephadmContext, str, str, Union[int, str], Optional[str]) -> Optional[str]
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


def get_unit_name(
    fsid: str, daemon_type: str, daemon_id: Union[str, int]
) -> str:
    """Return the name of the systemd unit given an fsid, a daemon_type,
    and the daemon_id.
    """
    # TODO: fully replace get_unit_name with DaemonIdentity instances
    return DaemonIdentity(fsid, daemon_type, daemon_id).unit_name


def get_file_timestamp(fn):
    # type: (str) -> Optional[str]
    try:
        mt = os.path.getmtime(fn)
        return datetime.datetime.fromtimestamp(
            mt, tz=datetime.timezone.utc
        ).strftime(DATEFMT)
    except Exception:
        return None


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


def get_container_stats(
    ctx: CephadmContext,
    container_path: str,
    fsid: str,
    daemon_type: str,
    daemon_id: str,
) -> Tuple[str, str, int]:
    """returns container id, image name, image id, created time, and ceph version if available"""
    c = CephContainer.for_daemon(
        ctx, DaemonIdentity(fsid, daemon_type, daemon_id), 'bash'
    )
    out, err, code = '', '', -1
    for name in (c.cname, c.old_cname):
        cmd = [
            container_path,
            'inspect',
            '--format',
            '{{.Id}},{{.Config.Image}},{{.Image}},{{.Created}},{{index .Config.Labels "io.ceph.version"}}',
            name,
        ]
        out, err, code = call(ctx, cmd, verbosity=CallVerbosity.QUIET)
        if not code:
            break
    return out, err, code


def get_container_info(
    ctx: CephadmContext, daemon_filter: str, by_name: bool
) -> Optional[ContainerInfo]:
    """
    :param ctx: Cephadm context
    :param daemon_filter: daemon name or type
    :param by_name: must be set to True if daemon name is provided
    :return: Container information or None
    """

    def daemon_name_or_type(daemon: Dict[str, str]) -> str:
        return daemon['name'] if by_name else daemon['name'].split('.', 1)[0]

    if by_name and '.' not in daemon_filter:
        logger.warning(
            f'Trying to get container info using invalid daemon name {daemon_filter}'
        )
        return None
    if by_name:
        matching_daemons = _get_matching_daemons_by_name(ctx, daemon_filter)
    else:
        # NOTE: we are passing detail=False here as in this case where we are not
        # doing it by_name, we really only need the names of the daemons. Additionally,
        # when not doing it by_name, we are getting the info for all daemons on the
        # host, and doing this with detail=True tends to be slow.
        daemons = list_daemons(ctx, detail=False)
        matching_daemons = [
            d
            for d in daemons
            if daemon_name_or_type(d) == daemon_filter
            and d['fsid'] == ctx.fsid
        ]
    if matching_daemons:
        if (
            by_name
            and 'state' in matching_daemons[0]
            and matching_daemons[0]['state'] != 'running'
            and 'container_image_name' in matching_daemons[0]
            and matching_daemons[0]['container_image_name']
        ):
            # this daemon contianer is not running so the regular `podman/docker inspect` on the
            # container will not help us. If we have the image name from the list_daemons output
            # we can try that.
            image_name = matching_daemons[0]['container_image_name']
            out, _, code = get_container_stats_by_image_name(
                ctx, ctx.container_engine.path, image_name
            )
            if not code:
                # keep in mind, the daemon container is not running, so no container id here
                (image_id, start, version) = out.strip().split(',')
                return ContainerInfo(
                    container_id='',
                    image_name=image_name,
                    image_id=image_id,
                    start=start,
                    version=version,
                )
        else:
            d_type, d_id = matching_daemons[0]['name'].split('.', 1)
            out, _, code = get_container_stats(
                ctx, ctx.container_engine.path, ctx.fsid, d_type, d_id
            )
            if not code:
                (
                    container_id,
                    image_name,
                    image_id,
                    start,
                    version,
                ) = out.strip().split(',')
                return ContainerInfo(
                    container_id, image_name, image_id, start, version
                )
    return None


def get_container_stats_by_image_name(
    ctx: CephadmContext, container_path: str, image_name: str
) -> Tuple[str, str, int]:
    """returns image id, created time, and ceph version if available"""
    out, err, code = '', '', -1
    cmd = [
        container_path,
        'image',
        'inspect',
        '--format',
        '{{.Id}},{{.Created}},{{index .Config.Labels "io.ceph.version"}}',
        image_name,
    ]
    out, err, code = call(ctx, cmd, verbosity=CallVerbosity.QUIET)
    return out, err, code
