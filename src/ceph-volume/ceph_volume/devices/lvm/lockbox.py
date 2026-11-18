import argparse
import json
import os
import sys
from textwrap import dedent
from typing import Any, Dict, List, Optional

from ceph_volume import decorators, process
from ceph_volume.api.lvm import Volume, get_lvs
from ceph_volume.util import system
from ceph_volume.util.constants import cephx_lockbox_secret_tag


def lockbox_keyring_path(cluster_name: str, osd_id: str) -> str:
    return '/var/lib/ceph/osd/%s-%s/lockbox.keyring' % (cluster_name, osd_id)


def validate_lockbox_secret(secret: str) -> None:
    # LVM tags are comma-separated, so a comma or whitespace would split the tag.
    if not secret or any(char in secret for char in ' ,\t\n'):
        raise RuntimeError('invalid lockbox secret')


def lockbox_lvs(osd_id: str, osd_fsid: str) -> List[Volume]:
    found = get_lvs(tags={'ceph.osd_id': osd_id, 'ceph.osd_fsid': osd_fsid})
    matched = []
    for lv in found:
        tags = lv.tags
        if tags.get('ceph.osd_id') != osd_id or tags.get('ceph.osd_fsid') != osd_fsid:
            continue
        if cephx_lockbox_secret_tag not in tags:
            continue
        matched.append(lv)
    if not matched:
        raise RuntimeError(
            'no LV with %s for osd.%s (%s)' % (cephx_lockbox_secret_tag, osd_id, osd_fsid)
        )
    return matched


def osd_cluster_name(lvs: List[Volume], osd_id: str) -> str:
    names = {lv.tags.get('ceph.cluster_name') for lv in lvs}
    if len(names) != 1:
        raise RuntimeError('inconsistent ceph.cluster_name for osd.%s' % osd_id)
    cluster_name = names.pop()
    if not cluster_name:
        raise RuntimeError('missing ceph.cluster_name for osd.%s' % osd_id)
    return cluster_name


def replace_lockbox_tag(lv: Volume, secret: str) -> None:
    current = lv.tags[cephx_lockbox_secret_tag]
    if current == secret:
        return
    _, _, returncode = process.call(
        [
            'lvchange',
            '--deltag', '%s=%s' % (cephx_lockbox_secret_tag, current),
            '--addtag', '%s=%s' % (cephx_lockbox_secret_tag, secret),
            lv.lv_path,
        ],
        run_on_host=True,
        # lvchange has to receive the tag values, but the logged command must not.
        obfuscate=['--deltag', '--addtag'],
    )
    if returncode != 0:
        raise RuntimeError('failed to set %s on %s' % (cephx_lockbox_secret_tag, lv.lv_path))
    lv.tags[cephx_lockbox_secret_tag] = secret


def sync_lockbox_keyring(path: str, osd_fsid: str, secret: str) -> Optional[str]:
    """Rewrite an existing lockbox keyring. A missing file is left absent."""
    if not os.path.exists(path):
        return None
    contents = '[client.osd-lockbox.%s]\n\tkey = %s\n' % (osd_fsid, secret)
    with open(path) as keyring:
        current = keyring.read()
    if current != contents:
        with open(path, 'w') as keyring:
            keyring.write(contents)
        os.chmod(path, 0o600)
        system.chown(path, recursive=False)
    return path


def set_lockbox_secret(osd_id: str, osd_fsid: str, secret: str) -> Dict[str, Any]:
    """Store a new lockbox secret on the LVs of one OSD and on its keyring."""
    validate_lockbox_secret(secret)
    lvs = lockbox_lvs(osd_id, osd_fsid)
    cluster_name = osd_cluster_name(lvs, osd_id)
    for lv in lvs:
        replace_lockbox_tag(lv, secret)
    keyring = sync_lockbox_keyring(
        lockbox_keyring_path(cluster_name, osd_id),
        osd_fsid,
        secret,
    )
    return {
        'osd_id': osd_id,
        'osd_fsid': osd_fsid,
        'lvs': sorted(lv.lv_path for lv in lvs),
        'lockbox_keyring': keyring,
    }


class SetLockboxSecret(object):

    help = 'Update the lockbox secret stored for an encrypted OSD'

    def __init__(self, argv: Optional[List[str]] = None) -> None:
        self.argv = argv

    @decorators.needs_root
    def main(self) -> None:
        sub_command_help = dedent("""
        Replace the lockbox cephx secret stored in the LVM tags of an encrypted
        OSD, and rewrite lockbox.keyring when that file already exists.

            ceph-volume lvm set-lockbox-secret --osd-id ID --osd-fsid FSID

        The new secret is read from stdin. This command does not contact the
        monitors.
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm set-lockbox-secret',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )
        parser.add_argument(
            '--osd-id',
            required=True,
            help='The ID of the OSD',
        )
        parser.add_argument(
            '--osd-fsid',
            required=True,
            help='The UUID of the OSD',
        )
        if self.argv is None:
            self.argv = []
        args = parser.parse_args(self.argv)
        secret = sys.stdin.read().strip()
        report = set_lockbox_secret(args.osd_id, args.osd_fsid, secret)
        print(json.dumps(report, sort_keys=True))
