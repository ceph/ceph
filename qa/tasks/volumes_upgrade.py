"""
mgr/volumes upgrade sub-tasks: verify that auto-upgrade of v1 and v2
subvolumes to the v3 layout does not disrupt subvolume clients doing IO, that
async clones and purges started before an upgrade finish successfully after
it, that v1/v2 subvolumes keep working as such while only ceph-mgr is
upgraded, that subvolume (and snapshot) attributes (of the attribute
profiles given in the config, see ATTRS), subvolume group attributes, auth
IDs and the
subvolume boundary survive auto-upgrade, that clones get the attributes of
their source, that sessions mounted at the
pre-v3 data dir can be evicted, that snapshots taken before and after
auto-upgrade are all listed, and that subvolumes removed with retained
snapshots (one of them with a pending clone) can be upgraded.

Run these in order (see qa/suites/fs/upgrade/volumes):

  - volumes_upgrade.setup             (on the old release)
  - volumes_upgrade.verify_mgr_only   (only after a mgr-only upgrade)
  - volumes_upgrade.check_io          (any time after setup)
  - volumes_upgrade.post_upgrade      (after the whole cluster is upgraded)
"""

import contextlib
import functools
import json
import logging
import re
import time
import uuid
from configparser import ConfigParser
from textwrap import dedent

from teuthology import misc
from teuthology.contextutil import safe_while
from teuthology.orchestra import run

log = logging.getLogger(__name__)

VOLNAME = 'cephfs'
SNAPNAME = 'snap0'
SNAPNAME_2 = 'snap1'
# taken (on the clone source) by the new mgr while the MDS is still old
SNAP_MGR_ONLY = 'snap_mgr_only'
# taken on every subvolume after auto-upgrade
SNAP_V3 = 'snap_v3'
TRASH_DIR = 'volumes/_deleting'
# mgr/volumes/snapshot_clone_delay the old ceph-mgr picks up the clones with,
# so that they're still pending when it gets upgraded (pause_cloning cancels
# clones, see https://tracker.ceph.com/issues/81383).
CLONE_DELAY = 86400
# mgr/volumes/max_concurrent_clones for the old ceph-mgr: each delayed clone
# holds a cloner thread, and with mgr/volumes/snapshot_clone_no_wait (the
# default) ceph-mgr rejects a clone (EAGAIN) once as many clones as there are
# cloner threads are pending.
MAX_CONCURRENT_CLONES = 8
STATE_KEY = 'volumes-upgrade-state'

# (name, group) of the subvolumes this task creates
IO_SUBVOLS = [('sv_io_0', None), ('sv_io_1', None), ('sv_io_2', 'grp_io')]
CLONE_SRC = ('sv_clone_src', 'grp_src')
# name prefix of the subvolumes (without a client, snapshots or clones) the
# config can add, for more attribute profiles
ATTR_SV_PREFIX = 'sv_attr_'
CLONES = [('sv_clone_0', None), ('sv_clone_1', None), ('sv_clone_2', 'grp_clone')]
PURGES = [('sv_purge_0', None), ('sv_purge_1', None), ('sv_purge_2', 'grp_purge')]
# number of pre-v3 snapshots taken on each IO subvolume, while its client
# is writing
IO_NSNAPS = [0, 1, 2]
# a clone (pending through the upgrade) of the first snapshot of an IO
# subvolume: (clone, index of the IO subvolume)
IO_CLONE = (('sv_clone_io', 'grp_clone'), 1)
# v2 subvolumes removed with --retain-snapshots (v1 subvolumes don't support
# it), with their data dirs caught mid-purge: one with a clone (of its
# retained snapshot) pending through the upgrade, and one without.
RETAINED_CLONE_SRC = ('sv_retained_src', 'grp_retained')
RETAINED_CLONE = ('sv_clone_retained', 'grp_clone')
RETAINED_PURGE = ('sv_retained_purge', 'grp_retained')
STATE_RETAINED = 'snapshot-retained'

# non-default attributes the v1/v2 (IO, clone source and sv_attr_*)
# subvolumes are created with (per their profile), which must survive
# auto-upgrade. the user xattr (as well as the earmark, being one) is to be
# copied over by auto-upgrade, unlike ceph.* vxattrs (quota and layout)
# which it must carry over otherwise.
SV_SIZE = 10 * 1024 ** 3
SV_UID = SV_GID = 1000
SV_MODE = 0o750
SV_EARMARK = 'nfs'
SV_METADATA = {'vu_key_0': 'vu_val_0', 'vu_key_1': 'vu_val_1'}
SV_XATTR = ('user.volumes_upgrade', 'vu_xattr_val')
SV_MAX_FILES = 1000000
# an ACL (also copied over, as xattrs) that leaves the mode as is
SV_ACL = 'u:1001:r-x,d:u:1001:r-x'
# a data pool added to the volume, for --pool_layout
DATA_POOL_2 = f'cephfs.{VOLNAME}.data2'
# mode of a subvolume created without --mode
DEFAULT_MODE = 0o755
# attributes the subvolume group of a subvolume with the 'group' attribute
# is created with
GRP_SIZE = 20 * 1024 ** 3
GRP_UID = GRP_GID = 2000
GRP_MODE = 0o750
# `fs subvolumegroup info` fields that must be as the old ceph-mgr reported
# them
GRP_INFO_KEPT = ('uid', 'gid', 'mode', 'data_pool', 'bytes_quota', 'created_at')

# The attributes an attribute profile (`profiles` in the setup config) is
# made of: the non-default attributes a v1/v2 subvolume is created with, as
# `fs subvolume create` options or set by hand on its data dir.
ATTRS = {
    'size': '--size SV_SIZE',
    'owner': '--uid SV_UID --gid SV_GID',
    'mode': '--mode SV_MODE',
    'namespace': '--namespace-isolated',
    'earmark': '--earmark SV_EARMARK',
    'pool': '--pool_layout DATA_POOL_2',
    'metadata': 'SV_METADATA (`fs subvolume metadata set`)',
    'xattr': 'SV_XATTR on the data dir',
    'max_files': 'ceph.quota.max_files = SV_MAX_FILES on the data dir',
    'acl': 'SV_ACL on the data dir',
    # the subvolume inherits the data pool and uid/gid (unless given its
    # own) of the group
    'group': 'in a group created with GRP_* attributes and DATA_POOL_2',
}
# vxattrs of a data dir that must survive auto-upgrade (see _fs_attrs())
FS_VXATTRS = ('ceph.dir.layout.pool', 'ceph.dir.layout.pool_namespace',
              'ceph.quota.max_bytes', 'ceph.quota.max_files')
# a file in the data dir for probing the subvolume boundary
BOUNDARY_FILE = 'vu_boundary'
# user metadata of each pre-v3 snapshot (the value being the snapshot name)
SNAP_METADATA_KEY = 'vu_snap_key'
# `fs subvolume snapshot info` fields that must be as the old ceph-mgr
# reported them
SNAP_INFO_KEPT = ('created_at', 'data_pool')
EARMARK_XATTR = 'user.ceph.subvolume.earmark'
# `fs subvolume info` fields that must be as the old ceph-mgr reported them
INFO_KEPT = ('type', 'uid', 'gid', 'mode', 'data_pool', 'pool_namespace',
             'bytes_quota', 'created_at', 'earmark')
# `fs subvolume info` fields a clone gets from (the snapshot of) its source
CLONE_INFO_KEPT = ('uid', 'gid', 'mode', 'data_pool', 'pool_namespace',
                   'bytes_quota', 'earmark')

# Runs on the client node as root, from within the subvolume mount. Writes
# files until the stop file appears, periodically re-verifying recent writes
# and doing namespace operations. Any failure (e.g., EACCES/ESTALE after the
# subvolume's data dir gets renamed during auto-upgrade) makes it exit
# non-zero. The checksum manifest is kept outside CephFS on purpose.
WRITER_SCRIPT = dedent('''\
    set -u
    mnt=$1
    state=$2
    manifest=$state/manifest
    fail() { echo "$(date -u +%FT%T) writer: $*" >&2; exit 1; }
    : > "$manifest" || fail "creating manifest"
    cd "$mnt" || fail "cd $mnt"
    mkdir -p io || fail "mkdir io"
    i=0
    while [ ! -e "$state/stop" ]; do
        i=$((i + 1))
        f=io/f.$i
        dd if=/dev/urandom of=$f.tmp bs=4k count=4 conv=fsync status=none || fail "write $f.tmp"
        mv $f.tmp $f || fail "rename $f.tmp"
        md5sum $f >> "$manifest" || fail "checksum $f"
        if [ $((i % 20)) -eq 0 ]; then
            tail -n 20 "$manifest" | md5sum -c --quiet || fail "verify at $i"
            mkdir io/d.$i && rmdir io/d.$i || fail "mkdir/rmdir io/d.$i"
            ls io > /dev/null || fail "readdir"
            stat . > /dev/null || fail "stat"
        fi
        echo $i > "$state/progress"
        sleep 0.5
    done
    md5sum -c --quiet "$manifest" || fail "final verify"
    n=$(find io -maxdepth 1 -name "f.*" ! -name "*.tmp" | wc -l)
    [ "$n" -eq "$i" ] || fail "found $n files, wrote $i"
    echo "writer: wrote and verified $i files"
''')


class Subvol:
    def __init__(self, name, group):
        self.name = name
        self.group = group
        self.base = f'volumes/{group or "_nogroup"}/{name}'
        # subvolume version before auto-upgrade to v3
        self.version = 2
        # path to the pre-v3 (v1/v2) data dir, relative to fs root
        self.data_path = None
        # name and attributes (see ATTRS) of the attribute profile it is
        # created with by _create_pre_v3(), if any
        self.profile = None
        self.attrs = set()
        # owner, mode and data pool it is created with (if profile)
        self.uid = self.gid = 0
        self.mode = DEFAULT_MODE
        self.pool = None
        # pool namespace it is isolated in (if profile)
        self.namespace = None
        # `fs subvolume info` by the old ceph-mgr (if profile and it could be
        # queried)
        self.info = None
        # attributes of the pre-v3 data dir as the file system has them
        # (see _fs_attrs()), at the end of setup (if profile)
        self.fs_attrs = None
        # removed with --retain-snapshots
        self.retained = False

    def has(self, attr):
        return attr in self.attrs

    @property
    def grp_arg(self):
        return f' --group_name {self.group}' if self.group else ''

    def __str__(self):
        return f'{self.group or "_nogroup"}/{self.name}'


class State:
    def __init__(self, ctx, config):
        self.manager = ctx.managers['ceph']
        self.admin = _get_mount(ctx, config.get('admin_client', 'client.3'))
        self.io_mounts = [_get_mount(ctx, c) for c in
                          config.get('io_clients',
                                     ['client.0', 'client.1', 'client.2'])]
        assert len(self.io_mounts) == len(IO_SUBVOLS)
        # version of the (non-clone, non-purged) subvolumes before upgrade
        self.version = int(config.get('version', 2))
        assert self.version in (1, 2), f'unsupported version {self.version}'
        self.testdir = misc.get_testdir(ctx)

        self.io_svs = [Subvol(*s) for s in IO_SUBVOLS]
        self.src = Subvol(*CLONE_SRC)
        self.attr_svs = _assign_profiles(config, self.io_svs + [self.src])
        # group name -> `fs subvolumegroup info` by the old ceph-mgr, of each
        # group created with GRP_* attributes
        self.groups = {}
        # clones pending through the upgrade
        self.clones = [Subvol(*s) for s in CLONES]
        # clone name -> (source subvolume, snapshot) of each of self.clones
        self.clone_of = {c.name: (self.src, SNAPNAME) for c in self.clones}
        self.purges = [Subvol(*s) for s in PURGES]
        # subvolumes removed with --retain-snapshots: a clone source and a
        # purge
        self.retained = [Subvol(*RETAINED_CLONE_SRC), Subvol(*RETAINED_PURGE)]

        self.clone_src_files = config.get('clone_src_files', 5000)
        self.purge_files = config.get('purge_files', 5000)
        # subvolume name -> names of snapshots it is expected to have
        self.snaps = {}
        # (subvolume name, snapshot name) -> manifest of the snapshot's data
        self.snap_manifests = {}
        # clones issued after setup: (clone, source, snapshot)
        self.late_clones = []
        # one entry per io_svs: dict(mount, sv, statedir, proc)
        self.writers = []
        # (subvolume name, snapshot name) -> `fs subvolume snapshot info` by
        # the old ceph-mgr
        self.snap_info = {}
        # subvolume name -> dict(auth_id, keyring, mnt, root) of an auth ID
        # authorized by the old ceph-mgr for an extra (ceph-fuse) session
        # mounted at the pre-v3 data dir, for checking authorized_list,
        # deauthorize and evict after auto-upgrade
        self.evict = {}

    @property
    def pre_v3_svs(self):
        """
        The v1/v2 subvolumes created with an attribute profile.
        """
        return self.io_svs + [self.src] + self.attr_svs


def _assign_profiles(config, svs):
    """
    Assign the attribute profiles (`profiles`: name -> list of attributes,
    see ATTRS) to the subvolumes (`subvolumes`: [<group>/]<name> -> profile
    name) per the config. Each of svs must be assigned one. Any other
    subvolume listed (named ATTR_SV_PREFIX*, not in a group of the task's
    other subvolumes) is created, with its profile, without a client,
    snapshots or clones; return those.
    """
    profiles = config.get('profiles') or {}
    assigned = config.get('subvolumes') or {}
    assert assigned, 'no subvolumes (with attribute profiles) in config'
    for pname, attrs in profiles.items():
        unknown = set(attrs or []) - ATTRS.keys()
        assert not unknown, f'profile {pname}: unknown attributes {unknown}'

    fixed = {str(sv): sv for sv in svs}
    keys = {str(Subvol(*_parse_sv_key(k))) for k in assigned}
    missing = [k for k in fixed if k not in keys]
    assert not missing, f'no profile for subvolumes {missing}'
    task_groups = {g for _, g in IO_SUBVOLS + [CLONE_SRC] + CLONES + PURGES +
                   [IO_CLONE[0], RETAINED_CLONE_SRC, RETAINED_CLONE, RETAINED_PURGE]
                   if g}
    extra = []
    group_attr = {}
    for key, pname in assigned.items():
        assert pname in profiles, f'{key}: unknown profile {pname}'
        sv = Subvol(*_parse_sv_key(key))
        if str(sv) in fixed:
            sv = fixed[str(sv)]
        else:
            assert sv.name.startswith(ATTR_SV_PREFIX), \
                f'{key}: not a subvolume of the task, nor named {ATTR_SV_PREFIX}*'
            assert sv.group not in task_groups, \
                f'{key}: in group {sv.group} of the task\'s other subvolumes'
            extra.append(sv)
        sv.profile, sv.attrs = pname, set(profiles[pname] or [])
        assert sv.group or not sv.has('group'), \
            f'{key}: profile {pname} has the group attribute, but no group'
        # a group is created with (or without) GRP_* attributes only once
        if sv.group:
            prev = group_attr.setdefault(sv.group, sv.has('group'))
            assert prev == sv.has('group'), \
                f'{key}: group {sv.group} with and without the group attribute'
    return extra


def _parse_sv_key(key):
    group, _, name = key.rpartition('/')
    return name, group or None


def _check_fragment_names(description, config):
    """
    Each subvolumes/<name>-<profile>.yaml fragment in the job description
    must assign <profile> to the subvolume <name>, and each subvolume
    assigned a profile must have such a fragment, so that the description
    tells which subvolume got which profile. Not checked if the description
    has no such fragments.
    """
    frags = []
    for m in re.finditer(r'(?<![\w-])subvolumes/(\{[^{}]*\}|[^\s{}]+)', description):
        frags += m.group(1).strip('{}').split()
    if not frags:
        log.info('no subvolumes/ fragments in the job description, not checking '
                 'their names')
        return
    per_names = {}
    for frag in frags:
        name, sep, profile = frag.rpartition('-')
        assert sep, f'subvolumes/{frag}: not named <subvolume>-<profile>'
        per_names[name] = profile
    per_config = {_parse_sv_key(k)[0]: p
                  for k, p in (config.get('subvolumes') or {}).items()}
    diff = _diff(per_names, per_config)
    assert not diff, \
        f'subvolume profiles per fragment names and per config differ: {diff}'


def _get_mount(ctx, role):
    _, _, id_ = misc.split_role(role)
    return ctx.mounts[id_]


def _state(ctx):
    return ctx[STATE_KEY]


def _ceph(st, cmd, check_status=True):
    p = st.manager.ceph(cmd, check_status=check_status)
    return p.exitstatus, p.stdout.getvalue()


def _admin_sh(st, payload, timeout=900):
    """
    Run payload as root via bash, from the root of the admin mount. The
    payload must not contain single quotes.
    """
    p = st.admin.run_shell_payload(payload, sudo=True, timeout=timeout)
    return p.stdout.getvalue()


def _wait_for(desc, pred, timeout, sleep=5):
    with safe_while(sleep=sleep, tries=max(1, timeout // sleep),
                    action=desc) as proceed:
        while proceed():
            if pred():
                return


def _set_mgr_config(st, name, value):
    _ceph(st, f'config set mgr mgr/volumes/{name} {value}')
    # config_notify() is asynchronous
    time.sleep(5)


def _set_purging_paused(st, paused):
    _set_mgr_config(st, 'pause_purging', str(paused).lower())


def _count_files(st, path):
    out = _admin_sh(st, f'test -d {path} && find {path} -type f | wc -l || echo 0')
    return int(out.strip())


def _read_meta(st, sv, meta='.meta'):
    out = _admin_sh(st, f'cat {sv.base}/{meta}')
    cp = ConfigParser()
    cp.read_string(out)
    return cp


def _info(st, sv):
    _, out = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}')
    return json.loads(out)


def _diff(expected, actual):
    """
    Return {key: (expected, actual)} for each key whose values differ.
    """
    return {k: (expected.get(k), actual.get(k))
            for k in sorted(expected.keys() | actual.keys())
            if expected.get(k) != actual.get(k)}


def _getpath(st, sv, check_status=True):
    rc, out = _ceph(st, f'fs subvolume getpath {VOLNAME} {sv.name}{sv.grp_arg}',
                    check_status=check_status)
    return rc, out.strip()


def _manifest(st, path):
    return _admin_sh(st, f'cd {path} && find . -type f -print0 | sort -z | '
                         'xargs -0 -r md5sum')


def _populate(st, path, nfiles, files_per_dir=100):
    ndirs = max(1, nfiles // files_per_dir)
    _admin_sh(st, dedent(f'''\
        set -e
        cd {path}
        for d in $(seq 1 {ndirs}); do
            mkdir -p d.$d
            for f in $(seq 1 {files_per_dir}); do
                head -c 4096 /dev/urandom > d.$d/f.$f
            done
        done
    '''), timeout=3600)


# ----- subvolume layout checks -----


def _assert_pre_v3(st, sv, strict=False):
    """
    The subvolume is in its pre-v3 (v1/v2) layout and untouched by any
    (failed) auto-upgrade attempt.

    ceph-mgr has long been upgrading (the metadata of) v1 subvolumes without
    snapshots to v2 on access, the layout being the same. Unless strict, such
    a v1 subvolume may be either.
    """
    ftype = _admin_sh(st, f'stat -c %F {sv.base}/.meta').strip()
    assert ftype == 'regular file', f'{sv}: .meta is a {ftype}'
    meta = _read_meta(st, sv)
    version = meta.get('GLOBAL', 'version')
    expected = {str(sv.version)}
    if sv.version == 1 and not strict and not st.snaps.get(sv.name):
        expected.add('2')
    assert version in expected, f'{sv}: version = {version}, expected {expected}'
    path = meta.get('GLOBAL', 'path')
    assert path == f'/{sv.data_path}', f'{sv}: meta path = {path}'
    out = _admin_sh(st, f'test -d {sv.data_path} && echo yes; '
                        f'test -e {sv.base}/roots && echo yes || echo no')
    assert out.split() == ['yes', 'no'], \
        f'{sv}: half-upgraded subvolume (v2 dir, roots dir present?: {out.split()})'


def _assert_v3(st, sv):
    """
    Trigger auto-upgrade (if not done already) through getpath and verify the
    subvolume is in v3 layout. A v1/v2 subvolume, with or without snapshots,
    is upgraded in place: its data dir is moved under roots/ keeping its uuid
    (so that clients need not remount), along with any v1 snapshots (taken
    in the data dir), while v2 snapshots stay where they were
    (<subvolume>/.snap).
    """
    _, path = _getpath(st, sv)
    m = re.fullmatch(rf'/{re.escape(sv.base)}/roots/([0-9a-f-]{{36}})/mnt', path)
    assert m, f'{sv}: getpath returned {path}, expected a v3 path'
    sv_uuid = m.group(1)

    link = _admin_sh(st, f'readlink {sv.base}/.meta').strip()
    assert link == f'.meta.{sv_uuid}', f'{sv}: .meta -> {link}, uuid = {sv_uuid}'

    meta = _read_meta(st, sv)
    glob = dict(meta.items('GLOBAL'))
    log.info(f'{sv}: v3 meta = {glob}')
    assert glob.get('version') == '3', f'{sv}: meta = {glob}'
    assert glob.get('path') == path, f'{sv}: meta = {glob}'
    assert glob.get('state') == 'complete', f'{sv}: meta = {glob}'

    if sv.data_path:
        old_uuid = sv.data_path.rsplit('/', 1)[1]
        assert sv_uuid == old_uuid, f'{sv}: uuid changed {old_uuid} -> {sv_uuid}'
        old_dir = _admin_sh(st, f'test -e {sv.data_path} && echo yes || echo no').strip()
        assert old_dir == 'no', \
            f'{sv}: v{sv.version} data dir {sv.data_path} still exists'
    return path.lstrip('/')


def _assert_usable(st, sv):
    """
    The subvolume is still v1/v2 and subvolume commands on it work as usual.
    """
    _, path = _getpath(st, sv)
    assert path == f'/{sv.data_path}', f'{sv}: getpath returned {path}'
    _, out = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}')
    info = json.loads(out)
    assert info['path'] == f'/{sv.data_path}', f'{sv}: info = {info}'
    _assert_snaps(st, sv)
    _assert_pre_v3(st, sv)
    _assert_attrs(st, sv, sv.data_path)


# ----- subvolume attributes -----


def _assert_attrs(st, sv, path):
    """
    The subvolume (whose data dir is at path) has the attributes it was
    created with: as the file system has them on its data dir (compared with
    those of its pre-v3 data dir at the end of setup), as reported by
    `fs subvolume info` (also compared with what the old ceph-mgr reported,
    if it could be queried), and its user metadata.
    """
    if not sv.profile:
        return
    if sv.fs_attrs is not None:
        diff = _diff(sv.fs_attrs, _fs_attrs(st, path))
        assert not diff, \
            f'{sv}: data dir attributes changed on {path} (expected, actual): {diff}'

    info = _info(st, sv)
    log.info(f'{sv}: info = {info}')
    expected = _expected_info(sv)
    if sv.info:
        expected.update({k: sv.info.get(k) for k in INFO_KEPT})
        # v1 subvolumes gain features on getting upgraded
        if sv.version == 2:
            expected['features'] = sv.info.get('features')
    actual = {k: info.get(k) for k in expected}
    for d in (expected, actual):
        d['mode'] &= 0o7777
    diff = _diff(expected, actual)
    assert not diff, f'{sv}: attributes changed (expected, actual): {diff}'

    _, out = _ceph(st, f'fs subvolume metadata ls {VOLNAME} {sv.name}{sv.grp_arg}')
    metadata = json.loads(out)
    expected = SV_METADATA if sv.has('metadata') else {}
    assert metadata == expected, f'{sv}: metadata = {metadata}, expected {expected}'


def _expected_info(sv):
    """
    `fs subvolume info` fields as per the subvolume's profile.
    """
    expected = dict(uid=sv.uid, gid=sv.gid, mode=sv.mode, data_pool=sv.pool)
    if sv.has('namespace'):
        expected['pool_namespace'] = sv.namespace
    if sv.has('size'):
        expected['bytes_quota'] = SV_SIZE
    if sv.has('earmark'):
        expected['earmark'] = SV_EARMARK
    return expected


def _fs_attrs(st, path):
    """
    Return the attributes of a data dir as the file system has them: owner
    and mode, the layout and quota vxattrs (empty if not set), and all the
    user xattrs and POSIX ACLs (hex encoded). Times are left out, as are
    security.* xattrs (e.g., SELinux labels), which users don't set.
    """
    vxattrs = ''.join(f'echo "{x}=$(getfattr --only-values -n {x} {path} 2>/dev/null)"\n'
                      for x in FS_VXATTRS)
    out = _admin_sh(st, f'stat -c "%u %g %a" {path}\n{vxattrs}'
                        f'getfattr -d -m - -e hex --absolute-names {path}\n')
    lines = out.splitlines()
    uid, gid, mode = lines[0].split()
    attrs = dict(uid=uid, gid=gid, mode=mode)
    for line in lines[1:]:
        name, sep, val = line.partition('=')
        if sep and (name in FS_VXATTRS or name.startswith(('user.', 'system.'))):
            attrs[name] = val
    return attrs


def _record_fs_attrs(st, sv):
    """
    Record the attributes of the pre-v3 data dir as the file system has
    them, which must be as per the subvolume's profile.
    """
    attrs = _fs_attrs(st, sv.data_path)
    log.info(f'{sv}: data dir attributes = {attrs}')
    expected = {'uid': str(sv.uid), 'gid': str(sv.gid), 'mode': f'{sv.mode:o}',
                'ceph.dir.layout.pool': sv.pool}
    if sv.has('namespace'):
        expected['ceph.dir.layout.pool_namespace'] = sv.namespace
    if sv.has('size'):
        expected['ceph.quota.max_bytes'] = str(SV_SIZE)
    if sv.has('max_files'):
        expected['ceph.quota.max_files'] = str(SV_MAX_FILES)
    if sv.has('xattr'):
        expected[SV_XATTR[0]] = '0x' + SV_XATTR[1].encode().hex()
    diff = {k: (v, attrs.get(k)) for k, v in expected.items() if attrs.get(k) != v}
    assert not diff, f'{sv}: data dir attributes (expected, actual): {diff}'
    sv.fs_attrs = attrs


def _getfacl(st, path):
    return _admin_sh(st, f'getfacl -cpn {path}')


def _set_data_dir_attrs(st, sv):
    """
    Set the attributes (of the subvolume's profile) that are set on the data
    dir by hand regardless of the subvolume version (no ceph-mgr command sets
    them).
    """
    cmds = ['set -e']
    if sv.has('xattr'):
        cmds.append(f'setfattr -n {SV_XATTR[0]} -v {SV_XATTR[1]} {sv.data_path}')
    if sv.has('max_files'):
        cmds.append(f'setfattr -n ceph.quota.max_files -v {SV_MAX_FILES} {sv.data_path}')
    if sv.has('acl'):
        cmds.append(f'setfacl -m {SV_ACL} {sv.data_path}')
    cmds.append(f'touch {sv.data_path}/{BOUNDARY_FILE}')
    _admin_sh(st, '\n'.join(cmds))
    if sv.has('acl'):
        acl = _getfacl(st, sv.data_path)
        assert 'user:1001:r-x' in acl and 'default:user:1001:r-x' in acl, \
            f'{sv}: ACL not set:\n{acl}'


def _probe_boundary(st, sv, path):
    """
    Hard link a file in the data dir (at path) to the subvolume group dir,
    the subvolume dir, and (for v3) the incarnation dir, returning whether
    each is allowed ('ok') or crosses a subvolume boundary ('EXDEV'), so as
    to find where the subvolume is marked (ceph.dir.subvolume).
    """
    targets = {'group': sv.base.rsplit('/', 1)[0], 'subvolume': sv.base}
    if path.endswith('/mnt'):
        targets['incarnation'] = path.rsplit('/', 1)[0]
    result = {}
    for what, d in targets.items():
        dst = f'{d}/{BOUNDARY_FILE}.{sv.name}.link'
        out = _admin_sh(st, f'ln {path}/{BOUNDARY_FILE} {dst} 2>&1 && '
                            f'rm -f {dst} && echo ok || true').strip()
        if out != 'ok':
            assert 'cross-device' in out, f'{sv}: ln to {d}: {out}'
            out = 'EXDEV'
        result[what] = out
    log.info(f'{sv}: subvolume boundary probe from {path}: {result}')
    return result


def _record_attrs(st, sv):
    """
    Record the attributes of the subvolume as reported by the (old)
    ceph-mgr.
    """
    sv.info = _info(st, sv)


def _create_group(st, group, attrs=False):
    """
    Create a subvolume group, with the GRP_* attributes and DATA_POOL_2 if
    attrs.
    """
    cmd = f'fs subvolumegroup create {VOLNAME} {group}'
    if attrs:
        cmd += (f' --size {GRP_SIZE} --pool_layout {DATA_POOL_2} '
                f'--uid {GRP_UID} --gid {GRP_GID} --mode {GRP_MODE:o}')
        st.groups[group] = None
    _ceph(st, cmd)


def _assert_groups(st):
    """
    The subvolume groups created with GRP_* attributes have them, as the old
    ceph-mgr reported them (once recorded).
    """
    for group, recorded in st.groups.items():
        _, out = _ceph(st, f'fs subvolumegroup info {VOLNAME} {group}')
        info = json.loads(out)
        log.info(f'group {group}: info = {info}')
        expected = dict(uid=GRP_UID, gid=GRP_GID, mode=GRP_MODE,
                        data_pool=DATA_POOL_2, bytes_quota=GRP_SIZE)
        if recorded:
            expected.update({k: recorded.get(k) for k in GRP_INFO_KEPT})
        actual = {k: info.get(k) for k in expected}
        for d in (expected, actual):
            d['mode'] &= 0o7777
        diff = _diff(expected, actual)
        assert not diff, \
            f'group {group}: attributes changed (expected, actual): {diff}'


def _record_groups(st):
    for group in st.groups:
        _, out = _ceph(st, f'fs subvolumegroup info {VOLNAME} {group}')
        st.groups[group] = json.loads(out)


def _assert_clone_attrs(st, c, src):
    """
    The clone has the attributes of its source (those of the snapshot cloned
    being the same), data pool included, as the old ceph-mgr reported them.
    Not checked for a source without them recorded (e.g., one removed with
    retained snapshots, which `fs subvolume info` doesn't report them for).
    """
    if not src.info:
        return
    info = _info(st, c)
    expected = {k: src.info.get(k) for k in CLONE_INFO_KEPT}
    actual = {k: info.get(k) for k in CLONE_INFO_KEPT}
    for d in (expected, actual):
        d['mode'] &= 0o7777
    diff = _diff(expected, actual)
    assert not diff, \
        f'clone {c}: attributes differ from source {src} (expected, actual): {diff}'


# ----- snapshots -----


def _pre_v3_snap_path(sv, snap, upgraded=False):
    sv_uuid = sv.data_path.rsplit('/', 1)[1]
    if sv.version == 1:
        # v1 snapshots are taken in the data (uuid) dir, and move along with
        # it on auto-upgrade
        if upgraded:
            return f'{sv.base}/roots/{sv_uuid}/mnt/.snap/{snap}'
        return f'{sv.data_path}/.snap/{snap}'
    # v2 snapshots are taken a level above the data dir, and stay there
    return f'{sv.base}/.snap/{snap}/{sv_uuid}'


def _snap_create(st, sv, snap, pre_v3=False, direct=False):
    """
    Snapshot the subvolume through ceph-mgr, or directly (pre_v3 only), the
    way ceph-mgr would have.
    """
    if direct:
        assert pre_v3
        _admin_sh(st, f'mkdir {_pre_v3_snap_path(sv, snap)}')
    else:
        _ceph(st, f'fs subvolume snapshot create {VOLNAME} {sv.name} {snap}{sv.grp_arg}')
    st.snaps.setdefault(sv.name, set()).add(snap)
    if pre_v3:
        path = _pre_v3_snap_path(sv, snap)
        out = _admin_sh(st, f'test -d {path} && echo yes || echo no').strip()
        assert out == 'yes', f'{sv}: v{sv.version} snapshot {snap} not at {path}'
        st.snap_manifests[(sv.name, snap)] = _manifest(st, path)


def _assert_snaps(st, sv, upgraded=False):
    """
    Snapshot listing has exactly the snapshots taken so far (v1/v2 as well
    as v3 ones), and each of them can be queried. upgraded is whether the
    subvolume has been auto-upgraded.
    """
    _, out = _ceph(st, f'fs subvolume snapshot ls {VOLNAME} {sv.name}{sv.grp_arg}')
    names = {s['name'] for s in json.loads(out)}
    expected = st.snaps.get(sv.name, set())
    assert names == expected, f'{sv}: snapshots = {names}, expected {expected}'
    for snap in names:
        info = _snap_info(st, sv, snap)
        recorded = st.snap_info.get((sv.name, snap))
        if recorded:
            diff = {k: (recorded.get(k), info.get(k)) for k in SNAP_INFO_KEPT
                    if recorded.get(k) != info.get(k)}
            assert not diff, \
                f'{sv}: snapshot {snap} info changed (expected, actual): {diff}'
        if (sv.name, snap) in st.snap_manifests and not sv.retained:
            # a pre-v3 snapshot (snapshot metadata of a subvolume with
            # retained snapshots can't be queried)
            _, out = _ceph(st, f'fs subvolume snapshot metadata ls {VOLNAME} '
                               f'{sv.name} {snap}{sv.grp_arg}')
            metadata = json.loads(out)
            assert metadata == {SNAP_METADATA_KEY: snap}, \
                f'{sv}: snapshot {snap} metadata = {metadata}'
        if (sv.name, snap) in st.snap_manifests:
            _assert_snap_getpath(st, sv, snap, upgraded)


def _assert_snap_getpath(st, sv, snap, upgraded):
    """
    `fs subvolume snapshot getpath` of a pre-v3 snapshot returns where the
    snapshot is, before and after auto-upgrade, and the snapshot there has
    the data it was taken with. Where the snapshots of a subvolume removed
    with --retain-snapshots are after auto-upgrade is unspecified, so that
    path is only logged.
    """
    _, out = _ceph(st, f'fs subvolume snapshot getpath {VOLNAME} {sv.name} '
                       f'{snap}{sv.grp_arg}')
    path = out.strip()
    if sv.retained and upgraded:
        log.info(f'{sv}: snapshot {snap} getpath returned {path}')
    else:
        expected = f'/{_pre_v3_snap_path(sv, snap, upgraded)}'
        assert path == expected, \
            f'{sv}: snapshot {snap} getpath returned {path}, expected {expected}'
    assert _manifest(st, path.lstrip('/')) == st.snap_manifests[(sv.name, snap)], \
        f'{sv}: data at {path} does not match snapshot {snap}'


def _snap_info(st, sv, snap):
    _, out = _ceph(st, f'fs subvolume snapshot info {VOLNAME} {sv.name} {snap}{sv.grp_arg}')
    return json.loads(out)


def _record_snap_info(st, sv):
    for snap in st.snaps.get(sv.name, set()):
        st.snap_info[(sv.name, snap)] = _snap_info(st, sv, snap)


def _assert_pending_clones(st, sv, snap, expected):
    info = _snap_info(st, sv, snap)
    assert info.get('has_pending_clones') == expected, \
        f'{sv}: snapshot {snap} info = {info}, expected has_pending_clones = {expected}'


# ----- IO writers -----


def _start_writer(st, mount, sv):
    statedir = f'{st.testdir}/volumes_upgrade.io.{sv.name}'
    script = f'{st.testdir}/volumes_upgrade.writer.sh'
    remote = mount.client_remote
    remote.run(args=['sudo', 'rm', '-rf', statedir])
    remote.run(args=['mkdir', '-p', statedir])
    remote.write_file(script, WRITER_SCRIPT)
    proc = remote.run(
        args=['sudo', 'stdin-killer', '--timeout=60', '--',
              'bash', script, mount.mountpoint, statedir],
        wait=False, stdin=run.PIPE, label=f'writer {sv}')
    return dict(mount=mount, sv=sv, statedir=statedir, proc=proc)


def _writer_progress(w):
    if w['proc'].finished:
        # surfaces the writer's exit status
        w['proc'].wait()
        raise RuntimeError(f'writer for {w["sv"]} exited prematurely')
    remote = w['mount'].client_remote
    out = remote.sh(f'cat {w["statedir"]}/progress 2>/dev/null || echo 0')
    return int(out.strip())


def _wait_for_writer_progress(w, nfiles, timeout=300):
    start = _writer_progress(w)
    _wait_for(f'writer for {w["sv"]} to write {nfiles} files past {start}',
              lambda: _writer_progress(w) >= start + nfiles, timeout)


def _remount(st, mount, sv, path, **kwargs):
    """
    Remount the client at the subvolume's data dir (path), and restore the
    data dir's mode, which the mount helpers set to 1777 (on the mount root)
    after mounting. The five digits clear the sticky bit too.
    """
    mount.remount(cephfs_mntpt=f'/{path}', **kwargs)
    _admin_sh(st, f'chmod {sv.mode:05o} {path}')


def _remount_and_verify(st, w, path, client_id, **creds):
    """
    Remount the client at the subvolume's (new) v3 path with the given
    credentials (client_keyring or client_keyring_path), and verify that all
    the data written by the writer is accessible and intact, and that the
    subvolume is writable.
    """
    mount, sv = w['mount'], w['sv']
    log.info(f'{sv}: remounting as client.{client_id} at /{path}')
    _remount(st, mount, sv, path, client_id=client_id, **creds)
    manifest = f'{w["statedir"]}/manifest'
    mount.run_shell(['sudo', 'md5sum', '-c', '--quiet', manifest], timeout=1800)
    nlines = int(mount.client_remote.sh(f'wc -l < {manifest}').strip())
    found = int(mount.get_shell_stdout(
        'sudo find io -maxdepth 1 -name "f.*" ! -name "*.tmp" | wc -l').strip())
    assert found == nlines, f'{sv}: {found} files after remount, expected {nlines}'
    mount.run_shell(['sudo', 'bash', '-c',
                     f'echo {client_id} > io/post-remount && '
                     f'grep -q {client_id} io/post-remount && '
                     'rm io/post-remount'])


def _kill_writers(st):
    for w in st.writers:
        if not w['proc'].finished:
            log.info(f'killing writer for {w["sv"]}')
            w['mount'].client_remote.run(
                args=['sudo', 'pkill', '-KILL', '-f', 'volumes_upgrade.writer.sh'],
                check_status=False)


def _kill_writers_on_failure(func):
    """
    Writers left running (in the subvolume mounts) make the client mounts
    fail to unmount on teardown, as do the extra (ceph-fuse) sessions'
    mounts the test dir removal.
    """
    @functools.wraps(func)
    def wrapper(ctx, config):
        try:
            return func(ctx, config)
        except Exception:
            try:
                _kill_writers(_state(ctx))
            except Exception:
                log.exception('failed to kill writers')
            try:
                _cleanup_evict_mounts(_state(ctx))
            except Exception:
                log.exception('failed to unmount extra sessions')
            raise
    return wrapper


def _stop_writer(w):
    remote = w['mount'].client_remote
    remote.run(args=['sudo', 'touch', f'{w["statedir"]}/stop'])
    _wait_for(f'writer for {w["sv"]} to stop',
              lambda: w['proc'].finished, timeout=900)
    w['proc'].stdin.close()
    w['proc'].wait()
    return int(remote.sh(f'cat {w["statedir"]}/progress').strip())


# ----- auth IDs and evict -----


def _authorized_ids(st, sv):
    _, out = _ceph(st, f'fs subvolume authorized_list {VOLNAME} {sv.name}{sv.grp_arg}')
    return {k for e in json.loads(out) for k in e}


def _mgr_auth_ids(st, i, post=False):
    """
    Auth IDs authorized for IO subvolume i through ceph-mgr (with
    `fs subvolume authorize`): those of the client (v2 only, v1 credentials
    being made by hand), of the extra session (if any), and the client's
    new one (post auto-upgrade).
    """
    sv = st.io_svs[i]
    ids = set()
    if st.version == 2:
        ids.add(f'svio{i}')
    if sv.name in st.evict:
        ids.add(st.evict[sv.name]['auth_id'])
    if post:
        ids.add(f'svio{i}new')
    return ids


def _assert_authorized(st, post=False):
    for i, sv in enumerate(st.io_svs):
        if st.version == 1 and not st.snaps.get(sv.name) and not post:
            # nothing authorized through ceph-mgr, see _setup_evict()
            continue
        ids = _authorized_ids(st, sv)
        expected = _mgr_auth_ids(st, i, post)
        assert ids == expected, f'{sv}: authorized_list = {ids}, expected {expected}'


def _sessions(st, auth_id):
    """
    Return the roots of the sessions of auth_id across all active ranks.
    """
    _, out = _ceph(st, f'fs get {VOLNAME} --format=json')
    max_mds = json.loads(out)['mdsmap']['max_mds']
    roots = []
    for rank in range(max_mds):
        _, out = _ceph(st, f'tell mds.{VOLNAME}:{rank} client ls --format=json')
        for session in json.loads(out):
            md = session.get('client_metadata', {})
            if md.get('entity_id') == auth_id:
                roots.append(md.get('root'))
    return roots


def _fuse_mount(st, auth_id, keyring_path, root, mnt, check_status=True):
    remote = st.admin.client_remote
    remote.run(args=['mkdir', '-p', mnt])
    p = remote.run(args=['sudo', 'timeout', '300', 'ceph-fuse',
                         '-c', '/etc/ceph/ceph.conf', '--id', auth_id,
                         '-k', keyring_path, '-r', f'/{root}', mnt],
                   check_status=check_status)
    return p.exitstatus


def _fuse_umount(st, mnt, auth_id=None):
    """
    Unmount an extra session's mount cleanly, so that ceph-fuse closes its
    session. A killed ceph-fuse leaves its session (and the caps it holds,
    e.g. on files the IO clients write) behind until the MDS autocloses it
    (after 300s). So, if it does not go away, evict its session (by
    auth_id).
    """
    remote = st.admin.client_remote
    # [c]: so as not to match the (remote) shell running pgrep/pkill
    pattern = f'[c]eph-fuse.*{mnt}'
    remote.run(args=['sudo', 'umount', mnt], check_status=False)

    def gone():
        return remote.run(args=['pgrep', '-f', pattern],
                          check_status=False).exitstatus != 0
    try:
        _wait_for(f'ceph-fuse at {mnt} to exit', gone, timeout=60)
    except Exception:
        # ceph-fuse mounted before the MDS upgrade may not exit after a
        # (successful) unmount: https://tracker.ceph.com/issues/64502
        log.warning(f'ceph-fuse at {mnt} did not exit after unmount, killing it')
        remote.run(args=['sudo', 'umount', '-l', mnt], check_status=False)
        remote.run(args=['sudo', 'pkill', '-KILL', '-f', pattern],
                   check_status=False)
        if auth_id:
            _evict_sessions(st, auth_id)
    remote.run(args=['rmdir', mnt], check_status=False)


def _evict_sessions(st, auth_id):
    rc, out = _ceph(st, f'fs get {VOLNAME} --format=json', check_status=False)
    max_mds = json.loads(out)['mdsmap']['max_mds'] if rc == 0 else 1
    for rank in range(max_mds):
        _ceph(st, f'tell mds.{VOLNAME}:{rank} client evict '
                  f'client_metadata.entity_id={auth_id}', check_status=False)


def _cleanup_evict_mounts(st):
    for e in st.evict.values():
        _fuse_umount(st, e['mnt'], e['auth_id'])


def _setup_evict(st):
    """
    Authorize an auth ID for each IO subvolume through the old ceph-mgr, and
    mount the subvolume (pre-v3 data dir) with it using ceph-fuse on the
    admin client's node. A v1 subvolume without snapshots is skipped, as
    the old ceph-mgr would upgrade it to v2.
    """
    remote = st.admin.client_remote
    for i, sv in enumerate(st.io_svs):
        if sv.version == 1 and not st.snaps.get(sv.name):
            log.info(f'{sv}: v1 without snapshots, not authorizing through ceph-mgr')
            continue
        auth_id = f'svev{i}'
        keyring = _authorize(st, sv, auth_id)
        keyring_path = f'{st.testdir}/volumes_upgrade.{auth_id}.keyring'
        remote.write_file(keyring_path, keyring)
        mnt = f'{st.testdir}/volumes_upgrade.evict.{i}'
        st.evict[sv.name] = dict(auth_id=auth_id, keyring=keyring_path,
                                 mnt=mnt, root=sv.data_path)
        _fuse_mount(st, auth_id, keyring_path, sv.data_path, mnt)
        remote.run(args=['sudo', 'stat', f'{mnt}/{BOUNDARY_FILE}'])
        roots = set(_sessions(st, auth_id))
        assert roots == {f'/{sv.data_path}'}, f'{sv}: sessions of {auth_id}: {roots}'


def _verify_deauthorize_evict(st, i, path):
    """
    After auto-upgrade, deauthorizing the auth ID of the extra session must
    revoke its access to the subvolume (both the pre-v3 and v3 paths), and
    evicting it must evict the session, mounted at the pre-v3 data dir.
    """
    sv = st.io_svs[i]
    e = st.evict.get(sv.name)
    if not e:
        return
    auth_id, remote = e['auth_id'], st.admin.client_remote

    # the session survived the upgrade, and the subvolume is accessible
    roots = set(_sessions(st, auth_id))
    assert roots == {f'/{e["root"]}'}, f'{sv}: sessions of {auth_id}: {roots}'
    remote.run(args=['sudo', 'timeout', '300', 'stat', f'{e["mnt"]}/{BOUNDARY_FILE}'])

    _ceph(st, f'fs subvolume deauthorize {VOLNAME} {sv.name} {auth_id}{sv.grp_arg}')
    ids = _authorized_ids(st, sv)
    assert auth_id not in ids, f'{sv}: {auth_id} still in authorized_list {ids}'
    rc, out = _ceph(st, f'auth get client.{auth_id} --format=json', check_status=False)
    if rc == 0:
        mds_caps = json.loads(out)[0]['caps'].get('mds', '')
        for p in (e['root'], path):
            assert f'path=/{p}' not in mds_caps, \
                f'{sv}: deauthorized {auth_id} has caps for /{p}: {mds_caps}'

    _ceph(st, f'fs subvolume evict {VOLNAME} {sv.name} {auth_id}{sv.grp_arg}')
    _wait_for(f'sessions of {auth_id} to be evicted',
              lambda: not _sessions(st, auth_id), timeout=120)
    _fuse_umount(st, e['mnt'], auth_id)

    # the old key must not get access to the subvolume any more
    rc = _fuse_mount(st, auth_id, e['keyring'], path, e['mnt'], check_status=False)
    if rc == 0:
        _fuse_umount(st, e['mnt'], auth_id)
    assert rc != 0, f'{sv}: deauthorized {auth_id} could mount /{path}'
    _fuse_umount(st, e['mnt'], auth_id)
    del st.evict[sv.name]


# ----- async jobs -----


def _clone_state(st, sv):
    """
    Read the clone state from the .meta file directly: "fs clone status"
    itself opens the subvolume, triggering auto-upgrade.
    """
    return _read_meta(st, sv).get('GLOBAL', 'state')


def _clone(st, src, snap, c):
    cmd = f'fs subvolume snapshot clone {VOLNAME} {src.name} {snap} {c.name}{src.grp_arg}'
    if c.group:
        _ceph(st, f'fs subvolumegroup create {VOLNAME} {c.group}')
        cmd += f' --target_group_name {c.group}'
    _ceph(st, cmd)


def _wait_for_clone(st, c):
    def done():
        _, out = _ceph(st, f'fs clone status {VOLNAME} {c.name}{c.grp_arg}')
        status = json.loads(out)['status']
        assert status['state'] in ('pending', 'in-progress', 'complete'), \
            f'clone {c}: status = {status}'
        return status['state'] == 'complete'
    _wait_for(f'clone {c} to complete', done, timeout=1800, sleep=10)


def _assert_clone_data(st, c, src, snap, path=None):
    if path is None:
        path = _getpath(st, c)[1].lstrip('/')
    assert _manifest(st, path) == st.snap_manifests[(src.name, snap)], \
        f'clone {c}: data does not match snapshot {snap} of {src}'


def _trash_entries(st):
    out = _admin_sh(st, f'test -d {TRASH_DIR} && '
                        f'find {TRASH_DIR} -mindepth 1 -maxdepth 1 | wc -l || echo 0')
    return int(out.strip())


def _setup_clones(st):
    sv = st.src
    _create_pre_v3(st, sv)
    _populate(st, sv.data_path, st.clone_src_files)
    _pre_v3_snap_create(st, sv, SNAPNAME)
    # a second snapshot, with different contents
    _admin_sh(st, f'mkdir {sv.data_path}/extra && touch {sv.data_path}/extra/file')
    _pre_v3_snap_create(st, sv, SNAPNAME_2)

    # the old ceph-mgr's cloner threads pick up the clones and sleep on the
    # delay (with the clones pending) until it gets upgraded. the delay is
    # reset at the end of setup (by then, the clones have long been picked
    # up), so that the new ceph-mgr starts them right away.
    _set_mgr_config(st, 'max_concurrent_clones', MAX_CONCURRENT_CLONES)
    _set_mgr_config(st, 'snapshot_clone_delay', CLONE_DELAY)
    clones, st.clones = st.clones, []
    for c in clones:
        _clone(st, sv, SNAPNAME, c)
        _register_clone(st, c, sv, SNAPNAME)

    # a clone of an IO subvolume, whose client keeps writing to it
    (name, group), i = IO_CLONE
    io_sv, io_snap = st.io_svs[i], f'snap_v{st.version}_0'
    c = Subvol(name, group)
    _clone(st, io_sv, io_snap, c)
    _register_clone(st, c, io_sv, io_snap)

    # a clone of a subvolume that is then removed with --retain-snapshots
    # (in _setup_purges())
    rsv = st.retained[0]
    _create_retained(st, rsv, 500)
    c = Subvol(*RETAINED_CLONE)
    _clone(st, rsv, SNAPNAME, c)
    _register_clone(st, c, rsv, SNAPNAME)
    for src, snap in set(st.clone_of.values()):
        _assert_pending_clones(st, src, snap, 'yes')
    assert len(st.clones) <= MAX_CONCURRENT_CLONES


def _register_clone(st, c, src, snap):
    """
    Record a clone (issued with snapshot_clone_delay set) to be held pending
    through the upgrade.
    """
    c.data_path = _read_meta(st, c).get('GLOBAL', 'path').lstrip('/')
    assert _clone_state(st, c) == 'pending'
    st.clones.append(c)
    st.clone_of[c.name] = (src, snap)


def _wait_for_clones(st, verify):
    """
    Wait for the clones held pending through the upgrade to finish, and
    verify their data (verify(c) returns the clone's data dir) and
    attributes.
    """
    for c in st.clones:
        _wait_for_clone(st, c)
        src, snap = st.clone_of[c.name]
        _assert_clone_data(st, c, src, snap, path=verify(c))
        _assert_clone_attrs(st, c, src)
    for src, snap in set(st.clone_of.values()):
        _assert_pending_clones(st, src, snap, 'no')


def _assert_clones_pending(st):
    for c in st.clones:
        state = _clone_state(st, c)
        copied = _count_files(st, c.data_path)
        log.info(f'clone {c}: state = {state}, copied {copied}')
        assert state == 'pending' and copied == 0, \
            f'clone {c} was not held pending (state = {state}, copied {copied})'


def _setup_purges(st):
    for sv in st.purges:
        if sv.group:
            _ceph(st, f'fs subvolumegroup create {VOLNAME} {sv.group}')
        _ceph(st, f'fs subvolume create {VOLNAME} {sv.name}{sv.grp_arg}')
        _, path = _getpath(st, sv)
        sv.data_path = path.lstrip('/')
        _populate(st, sv.data_path, st.purge_files)
    _create_retained(st, st.retained[1], st.purge_files)

    _set_purging_paused(st, True)
    for sv in st.purges:
        _ceph(st, f'fs subvolume rm {VOLNAME} {sv.name}{sv.grp_arg}')
    # moves their data dirs (but not their snapshots) to the trash; the
    # (first) one has a clone of its snapshot pending
    for sv in st.retained:
        _ceph(st, f'fs subvolume rm {VOLNAME} {sv.name}{sv.grp_arg} '
                  '--retain-snapshots')
        sv.retained = True
        _assert_retained_pre_v3(st, sv)
    total = _count_files(st, TRASH_DIR)
    assert _trash_entries(st) == len(st.purges) + len(st.retained)

    # as with clones, catch the purges mid-way
    _set_purging_paused(st, False)
    _wait_for('purges to make progress',
              lambda: _count_files(st, TRASH_DIR) <= total * 0.9,
              timeout=600, sleep=1)
    _set_purging_paused(st, True)
    left = _count_files(st, TRASH_DIR)
    log.info(f'purges paused with {left}/{total} files and '
             f'{_trash_entries(st)} trash entries left')
    assert left > 0, 'purges were not caught mid-way, increase purge_files'


def _create_retained(st, sv, nfiles):
    """
    Create a (v2, regardless of the version of the other subvolumes) subvolume
    with a snapshot, to be removed with --retain-snapshots.
    """
    if sv.group:
        _ceph(st, f'fs subvolumegroup create {VOLNAME} {sv.group}')
    _ceph(st, f'fs subvolume create {VOLNAME} {sv.name}{sv.grp_arg}')
    sv.data_path = _getpath(st, sv)[1].lstrip('/')
    _populate(st, sv.data_path, nfiles)
    _pre_v3_snap_create(st, sv, SNAPNAME)
    _record_snap_info(st, sv)


def _assert_retained(st, sv, upgraded=False):
    """
    The subvolume, removed with --retain-snapshots, is listed (with its
    snapshots) and reported as such.
    """
    _, out = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}')
    info = json.loads(out)
    assert info.get('state') == STATE_RETAINED, f'{sv}: info = {info}'
    _, out = _ceph(st, f'fs subvolume ls {VOLNAME}{sv.grp_arg}')
    names = {s['name'] for s in json.loads(out)}
    assert sv.name in names, f'{sv}: not in subvolume ls: {names}'
    _assert_snaps(st, sv, upgraded)


def _assert_retained_pre_v3(st, sv):
    """
    The subvolume, removed with --retain-snapshots, is still v2 (its data dir
    gone, its snapshots in place), and is reported as such.
    """
    ftype = _admin_sh(st, f'stat -c %F {sv.base}/.meta').strip()
    assert ftype == 'regular file', f'{sv}: .meta is a {ftype}'
    glob = dict(_read_meta(st, sv).items('GLOBAL'))
    assert (glob.get('version'), glob.get('state'), glob.get('path')) == \
        ('2', STATE_RETAINED, ''), f'{sv}: meta = {glob}'
    out = _admin_sh(st, f'test -e {sv.data_path} && echo yes || echo no; '
                        f'test -e {sv.base}/roots && echo yes || echo no')
    assert out.split() == ['no', 'no'], \
        f'{sv}: data dir, roots dir present?: {out.split()}'
    for snap in st.snaps[sv.name]:
        path = _pre_v3_snap_path(sv, snap)
        out = _admin_sh(st, f'test -d {path} && echo yes || echo no').strip()
        assert out == 'yes', f'{sv}: retained snapshot {snap} not at {path}'
    _assert_retained(st, sv)


def _verify_retained(st, sv, clone=None):
    """
    After auto-upgrade: the retained snapshot can still be cloned (into
    clone, if given), and removing it, the last one, removes the subvolume.
    """
    _assert_retained(st, sv, upgraded=True)
    log.info(f'{sv}: after auto-upgrade:\n' +
             _admin_sh(st, f'ls -la {sv.base} {sv.base}/roots 2>&1 || true; '
                           f'cat {sv.base}/.meta'))
    if clone:
        _clone(st, sv, SNAPNAME, clone)
        _wait_for_clone(st, clone)
        _assert_clone_data(st, clone, sv, SNAPNAME, path=_assert_v3(st, clone))
        _assert_pending_clones(st, sv, SNAPNAME, 'no')
    _ceph(st, f'fs subvolume snapshot rm {VOLNAME} {sv.name} {SNAPNAME}{sv.grp_arg}')
    st.snaps[sv.name].discard(SNAPNAME)
    rc, out = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}',
                    check_status=False)
    assert rc != 0, f'{sv}: still exists after removing its last snapshot: {out}'
    _, out = _ceph(st, f'fs subvolume ls {VOLNAME}{sv.grp_arg}')
    names = {s['name'] for s in json.loads(out)}
    assert sv.name not in names, f'{sv}: still listed: {names}'


def _authorize(st, sv, auth_id):
    _ceph(st, f'fs subvolume authorize {VOLNAME} {sv.name} {auth_id}'
              f'{sv.grp_arg} --access_level rw')
    # kernel clients may not support the default key type of newer
    # releases; same as the kclient task.
    _ceph(st, f'auth rotate client.{auth_id} --key-type=aes', check_status=False)
    _, keyring = _ceph(st, f'auth get client.{auth_id}')
    return keyring


def _add_data_pool(st):
    """
    Add a data pool to the volume, for the subvolumes (and the group) with a
    non-default data pool.
    """
    _ceph(st, f'osd pool create {DATA_POOL_2}')
    _ceph(st, f'fs add_data_pool {VOLNAME} {DATA_POOL_2}')


def _setup_io(st):
    for i, (mount, sv) in enumerate(zip(st.io_mounts, st.io_svs)):
        _create_pre_v3(st, sv)
        # mount the subvolume using its own (path restricted) credentials
        keyring = _pre_v3_authorize(st, sv, f'svio{i}')
        _remount(st, mount, sv, sv.data_path, client_id=f'svio{i}',
                 client_keyring=keyring)
        st.writers.append(_start_writer(st, mount, sv))

    for w, nsnaps in zip(st.writers, IO_NSNAPS):
        _wait_for_writer_progress(w, 20)
        for j in range(nsnaps):
            _pre_v3_snap_create(st, w['sv'], f'snap_v{st.version}_{j}')
            _wait_for_writer_progress(w, 20)


# ----- pre-v3 subvolumes -----
#
# v1 subvolumes can't be created by any release that can be upgraded from,
# so they are fabricated in the layout ceph-mgr used to create them in. A
# v1 subvolume without snapshots must not be accessed through the old
# ceph-mgr before the upgrade, as it would upgrade it to v2 (the new
# ceph-mgr does the same when only it is upgraded, which is fine).


def _data_pool(st):
    return _admin_sh(st, 'getfattr --only-values -n ceph.dir.layout.pool .').strip()


def _create_v1(st, sv):
    """
    Create a v1 subvolume in the layout ceph-mgr used to (see
    SubvolumeV1.create()): data in <subvolume>/<uuid> (with the pool layout
    set on it), and a .meta file (version 1) in <subvolume>. It gets the
    attributes of its profile the way ceph-mgr would set them (`fs subvolume
    create` options, `fs subvolume metadata set`), and is marked a subvolume
    (on its data dir, see SubvolumeV1.mark_subvolume()).
    """
    sv.version = 1
    sv.data_path = f'{sv.base}/{uuid.uuid4()}'
    cmds = ['set -e',
            f'mkdir -p -m 755 {sv.data_path}',
            # ceph-mgr sets the pool layout (the --pool_layout or the
            # inherited one) on the data dir regardless
            f'setfattr -n ceph.dir.layout.pool -v {sv.pool} {sv.data_path}',
            f'setfattr -n ceph.dir.subvolume -v 1 {sv.data_path}']
    if sv.has('namespace'):
        # as ceph-mgr names it (SubvolumeBase.namespace)
        sv.namespace = f'fsvolumens__{sv.group or "_nogroup"}_{sv.name}'
        cmds.append('setfattr -n ceph.dir.layout.pool_namespace '
                    f'-v {sv.namespace} {sv.data_path}')
    if sv.has('size'):
        cmds.append(f'setfattr -n ceph.quota.max_bytes -v {SV_SIZE} {sv.data_path}')
    if sv.has('earmark'):
        cmds.append(f'setfattr -n {EARMARK_XATTR} -v {SV_EARMARK} {sv.data_path}')
    meta = (f'[GLOBAL]\\nversion = 1\\ntype = subvolume\\n'
            f'path = /{sv.data_path}\\nstate = complete\\n')
    if sv.has('metadata'):
        meta += ('\\n[USER_METADATA]\\n' +
                 ''.join(f'{k} = {v}\\n' for k, v in SV_METADATA.items()))
    cmds += [f'chown {sv.uid}:{sv.gid} {sv.data_path}',
             f'chmod {sv.mode:o} {sv.data_path}',
             f'printf "{meta}" > {sv.base}/.meta']
    _admin_sh(st, '\n'.join(cmds))
    log.info(f'{sv}: created v1 subvolume at {sv.data_path}:\n'
             f'{_admin_sh(st, f"cat {sv.base}/.meta")}')


def _create_pre_v3(st, sv):
    """
    Create a v1/v2 subvolume with the attributes of its profile.
    """
    in_attr_group = sv.has('group')
    if sv.group:
        _create_group(st, sv.group, attrs=in_attr_group)
    # what ceph-mgr sets them to when not given (owner: the group's)
    if sv.has('owner'):
        sv.uid, sv.gid = SV_UID, SV_GID
    elif in_attr_group:
        sv.uid, sv.gid = GRP_UID, GRP_GID
    if sv.has('mode'):
        sv.mode = SV_MODE
    sv.pool = DATA_POOL_2 if sv.has('pool') or in_attr_group else _data_pool(st)
    log.info(f'{sv}: creating v{st.version} subvolume with profile '
             f'{sv.profile} {sorted(sv.attrs)}')
    if st.version == 1:
        _create_v1(st, sv)
    else:
        opts = ''
        if sv.has('size'):
            opts += f' --size {SV_SIZE}'
        if sv.has('owner'):
            opts += f' --uid {SV_UID} --gid {SV_GID}'
        if sv.has('mode'):
            opts += f' --mode {SV_MODE:o}'
        if sv.has('namespace'):
            opts += ' --namespace-isolated'
        if sv.has('earmark'):
            opts += f' --earmark {SV_EARMARK}'
        if sv.has('pool'):
            opts += f' --pool_layout {DATA_POOL_2}'
        _ceph(st, f'fs subvolume create {VOLNAME} {sv.name}{sv.grp_arg}{opts}')
        if sv.has('metadata'):
            for k, v in SV_METADATA.items():
                _ceph(st, f'fs subvolume metadata set {VOLNAME} {sv.name} {k} {v}'
                          f'{sv.grp_arg}')
        sv.data_path = _getpath(st, sv)[1].lstrip('/')
        if sv.has('namespace'):
            sv.namespace = _admin_sh(
                st, 'getfattr --only-values -n ceph.dir.layout.pool_namespace '
                    f'{sv.data_path}').strip()
            assert sv.namespace, f'{sv}: no pool namespace'
    assert sv.version == st.version
    _set_data_dir_attrs(st, sv)
    # the data dir must be in a subvolume
    probe = _probe_boundary(st, sv, sv.data_path)
    assert probe['group'] == 'EXDEV', f'{sv}: not marked a subvolume: {probe}'


def _pre_v3_authorize(st, sv, auth_id):
    if st.version == 2:
        return _authorize(st, sv, auth_id)
    # same caps as `fs subvolume authorize` (on a v1 subvolume) gives
    namespace = f' namespace={sv.namespace}' if sv.namespace else ''
    _ceph(st, f'auth get-or-create client.{auth_id} mon "allow r" '
              f'mds "allow rw path=/{sv.data_path}" '
              f'osd "allow rw pool={sv.pool}{namespace}"')
    _ceph(st, f'auth rotate client.{auth_id} --key-type=aes', check_status=False)
    _, keyring = _ceph(st, f'auth get client.{auth_id}')
    return keyring


def _pre_v3_snap_create(st, sv, snap):
    """
    Snapshot a pre-v3 subvolume through the old ceph-mgr, except for the
    first snapshot of a v1 subvolume (taken directly in its data dir, as v1
    does), which keeps the old ceph-mgr from upgrading it to v2.
    """
    direct = sv.version == 1 and not st.snaps.get(sv.name)
    _snap_create(st, sv, snap, pre_v3=True, direct=direct)
    _ceph(st, f'fs subvolume snapshot metadata set {VOLNAME} {sv.name} {snap} '
              f'{SNAP_METADATA_KEY} {snap}{sv.grp_arg}')


def _teardown(st):
    """
    Stop whatever setup left running (writers, extra sessions) and remove the
    files it created in the test dir, so that the client mounts can be
    unmounted and the test dir removed, whether or not the job failed.
    """
    try:
        _kill_writers(st)
    except Exception:
        log.exception('failed to kill writers')
    try:
        _cleanup_evict_mounts(st)
    except Exception:
        log.exception('failed to unmount extra sessions')
    remotes = {m.client_remote for m in st.io_mounts + [st.admin]}
    for remote in remotes:
        # --one-file-system: don't descend into an extra session's mount
        # that failed to unmount
        remote.run(args=['sudo', 'bash', '-c',
                         f'rm -rf --one-file-system {st.testdir}/volumes_upgrade.*'],
                   check_status=False)


# ----- sub-tasks -----


@contextlib.contextmanager
def setup(ctx, config):
    """
    On the old release, create v1 or v2 (per `version`) subvolumes, each with
    an attribute profile (per `profiles` and `subvolumes`, see
    _assign_profiles()): with clients doing IO in them, a clone source, and
    ones without clients (sv_attr_*), pending (delayed)
    clones (of the clone source, of an IO subvolume and of a v2 subvolume
    removed with retained snapshots), and in-progress (paused) purges (of v2
    subvolumes, and of the data dirs of v2 subvolumes removed with retained
    snapshots, as the old ceph-mgr creates clones as v2, upgrades a v1
    subvolume to v2 before removing it, and can't retain v1 snapshots).

    Example::

        tasks:
        - volumes_upgrade.setup:
            version: 1
            io_clients: [client.0, client.1, client.2]
            admin_client: client.3
            profiles:
              bare: []
              full: [size, owner, mode, namespace, earmark, metadata,
                     xattr, max_files, acl]
              pool_layout: [pool, namespace]
              group: [group]
            subvolumes:
              sv_io_0: bare
              sv_io_1: full
              grp_io/sv_io_2: group
              grp_src/sv_clone_src: pool_layout
              grp_attr/sv_attr_0: full

    The config is merged with `overrides: volumes_upgrade:`, where
    qa/suites/fs/upgrade/volumes sets the profiles (overrides/profiles.yaml)
    and assigns them (overrides/subvolumes/<subvolume>-<profile>.yaml, see
    _check_fragment_names()).
    """
    config = config or {}
    overrides = ctx.config.get('overrides', {})
    misc.deep_merge(config, overrides.get('volumes_upgrade', {}))
    _check_fragment_names(ctx.config.get('description') or '', config)
    st = State(ctx, config)
    ctx[STATE_KEY] = st
    try:
        _setup(st)
        yield
    finally:
        _teardown(st)


def _setup(st):
    _add_data_pool(st)
    _setup_io(st)
    _setup_evict(st)
    _setup_clones(st)
    for sv in st.attr_svs:
        _create_pre_v3(st, sv)
    _setup_purges(st)
    _assert_clones_pending(st)
    for src, snap in set(st.clone_of.values()):
        _assert_pending_clones(st, src, snap, 'yes')
    _set_mgr_config(st, 'snapshot_clone_delay', 0)

    for sv in st.pre_v3_svs:
        _record_fs_attrs(st, sv)
        if sv.version == 1 and not st.snaps.get(sv.name):
            _assert_pre_v3(st, sv, strict=True)
        else:
            # the old ceph-mgr must handle a v1 subvolume with snapshots as v1
            # (checks the attributes too)
            _assert_usable(st, sv)
            _record_attrs(st, sv)
            _record_snap_info(st, sv)
    _assert_groups(st)
    _record_groups(st)
    _assert_authorized(st)
    for c in st.clones:
        _assert_pre_v3(st, c)
    _ceph(st, f'fs subvolume ls {VOLNAME}')


@_kill_writers_on_failure
def check_io(ctx, config):
    """
    Verify that the subvolume clients are still doing IO.
    """
    st = _state(ctx)
    for w in st.writers:
        _wait_for_writer_progress(w, 10)


@_kill_writers_on_failure
def verify_mgr_only(ctx, config):
    """
    With only ceph-mgr upgraded, auto-upgrade can't set
    ceph.dir.subvolume.prevpath on the old MDS. The v1/v2 subvolumes must
    keep working as such: client IO, subvolume commands, clones and purges.
    """
    st = _state(ctx)
    check_io(ctx, config)

    for sv in st.pre_v3_svs:
        _assert_usable(st, sv)
    _assert_groups(st)
    _assert_authorized(st)

    # a v1/v2 snapshot taken by the new mgr
    _pre_v3_snap_create(st, st.src, SNAP_MGR_ONLY)
    _assert_usable(st, st.src)

    # the new mgr does not honor pause_purging on startup, so the purges may
    # already be running. resume them explicitly anyway.
    _set_purging_paused(st, False)

    _wait_for('purges to finish with old MDS',
              lambda: _trash_entries(st) == 0, timeout=1800, sleep=10)
    for sv in st.purges:
        rc, _ = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}',
                      check_status=False)
        assert rc != 0, f'purged subvolume {sv} still exists'

    def verify(c):
        _assert_usable(st, c)
        return c.data_path
    _wait_for_clones(st, verify)
    for sv in st.retained:
        _assert_retained_pre_v3(st, sv)

    # a new clone of a v1/v2 snapshot, by the new mgr
    c = Subvol('sv_clone_mgr_only', 'grp_clone')
    _clone(st, st.src, SNAPNAME_2, c)
    _wait_for_clone(st, c)
    _assert_clone_data(st, c, st.src, SNAPNAME_2)
    _assert_clone_attrs(st, c, st.src)
    _assert_snaps(st, c)
    st.late_clones.append((c, st.src, SNAPNAME_2))
    for snap in (SNAPNAME, SNAPNAME_2):
        _assert_pending_clones(st, st.src, snap, 'no')

    for sv in st.io_svs:
        _assert_usable(st, sv)
    check_io(ctx, config)


@_kill_writers_on_failure
def post_upgrade(ctx, config):
    """
    With the whole cluster upgraded, verify auto-upgrade of the subvolumes
    while their clients do IO, that the clones and purges finish, and that
    snapshots taken before and after auto-upgrade are all listed.
    """
    st = _state(ctx)
    check_io(ctx, config)
    _set_purging_paused(st, False)

    # first subvolume command on each subvolume triggers its auto-upgrade;
    # the writers must carry on regardless.
    for w in st.writers:
        sv = w['sv']
        _assert_attrs(st, sv, _assert_v3(st, sv))
        _wait_for_writer_progress(w, 20)
    _assert_authorized(st)

    _wait_for_clones(st, lambda c: _assert_v3(st, c))
    for c, src, snap in st.late_clones:
        _assert_clone_data(st, c, src, snap, path=_assert_v3(st, c))
        _assert_clone_attrs(st, c, src)

    for sv in [st.src] + st.attr_svs:
        _assert_attrs(st, sv, _assert_v3(st, sv))
    _assert_groups(st)

    # where the upgraded subvolumes are marked as such must be the same as
    # for a subvolume created as v3
    fresh = Subvol('sv_fresh_v3', 'grp_io')
    _ceph(st, f'fs subvolume create {VOLNAME} {fresh.name}{fresh.grp_arg}')
    fresh_path = _assert_v3(st, fresh)
    _admin_sh(st, f'touch {fresh_path}/{BOUNDARY_FILE}')
    expected = _probe_boundary(st, fresh, fresh_path)
    for sv in st.pre_v3_svs:
        probe = _probe_boundary(st, sv, _getpath(st, sv)[1].lstrip('/'))
        assert probe == expected, \
            f'{sv}: subvolume boundary {probe}, expected {expected} (as for v3)'

    # v1/v2 snapshots must still be listed after auto-upgrade (with their
    # metadata and info intact, and getpath returning where they now are),
    # and listed along with v3 snapshots once those are taken.
    late = [c for c, _, _ in st.late_clones]
    for sv in st.pre_v3_svs + st.clones + late:
        _assert_snaps(st, sv, upgraded=True)
        _snap_create(st, sv, SNAP_V3)
        _assert_snaps(st, sv, upgraded=True)

    # clones of v1/v2 snapshots (taken pre-upgrade) must still work
    io_src = st.io_svs[-1]
    io_snap = f'snap_v{st.version}_{IO_NSNAPS[-1] - 1}'
    for c, src, snap in [(Subvol('sv_clone_post_0', None), st.src, SNAPNAME),
                         (Subvol('sv_clone_post_1', 'grp_clone'), st.src, SNAPNAME_2),
                         (Subvol('sv_clone_post_2', 'grp_clone'), io_src, io_snap)]:
        _clone(st, src, snap, c)
        _wait_for_clone(st, c)
        _assert_clone_data(st, c, src, snap, path=_assert_v3(st, c))
        _assert_clone_attrs(st, c, src)
    for snap in (SNAPNAME, SNAPNAME_2):
        _assert_pending_clones(st, st.src, snap, 'no')
    _assert_pending_clones(st, io_src, io_snap, 'no')

    _wait_for('purges to finish', lambda: _trash_entries(st) == 0,
              timeout=1800, sleep=10)
    for sv in st.purges:
        rc, _ = _ceph(st, f'fs subvolume info {VOLNAME} {sv.name}{sv.grp_arg}',
                      check_status=False)
        assert rc != 0, f'purged subvolume {sv} still exists'

    # the subvolumes removed with --retain-snapshots (whose data dirs have
    # been purged by now): clone the retained snapshot again, then remove it
    # (along with the subvolume)
    _verify_retained(st, st.retained[0], Subvol('sv_clone_post_retained', 'grp_clone'))
    _verify_retained(st, st.retained[1])
    _wait_for('subvolumes with retained snapshots to be purged',
              lambda: _trash_entries(st) == 0, timeout=1800, sleep=10)

    for w in st.writers:
        sv = w['sv']
        written = _stop_writer(w)
        path = _getpath(st, sv)[1].lstrip('/')
        found = _count_files(st, f'{path}/io')
        log.info(f'{sv}: writer wrote {written} files, found {found} at {path}')
        assert found == written, f'{sv}: wrote {written} files, found {found}'

    # clients may choose to remount, in which case they mount the new v3
    # path, with either the credentials they had been using (authorized for
    # the v2 path) or with ones authorized after the upgrade.
    for i, w in enumerate(st.writers):
        sv, mount = w['sv'], w['mount']
        path = _getpath(st, sv)[1].lstrip('/')
        _remount_and_verify(st, w, path, mount.client_id,
                            client_keyring_path=mount.client_keyring_path)

        auth_id = f'svio{i}new'
        keyring = _authorize(st, sv, auth_id)
        _, out = _ceph(st, f'auth get client.{auth_id} --format=json')
        mds_caps = json.loads(out)[0]['caps']['mds']
        assert f'path=/{path}' in mds_caps, \
            f'{sv}: caps of client.{auth_id} not for the v3 path: {mds_caps}'
        _remount_and_verify(st, w, path, auth_id, client_keyring=keyring)
    _assert_authorized(st, post=True)

    for i, sv in enumerate(st.io_svs):
        _verify_deauthorize_evict(st, i, _getpath(st, sv)[1].lstrip('/'))

    _ceph(st, f'fs subvolume ls {VOLNAME}')
