import errno
import json
import datetime
from typing import TYPE_CHECKING, Optional, Tuple
from ceph.cephadm.version_entry import UpgradeType, UpgradeStatus, CephVersionEntry

if TYPE_CHECKING:
    from .module import CephadmOrchestrator

# on disk key prefix
VERSION_HISTORY_KEY_PREFIX = "version_history/"


class VersionTracker:

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr

    def add_cluster_version(self, version: str, time: str, params: dict, status: UpgradeStatus) -> None:
        # Extracts config dump json and stores in config_dump variable
        try:
            ret, out, err = self.mgr.check_mon_command({
                'prefix': 'config dump',
                'format': 'json',
            })
            config_dump = json.loads(out) if out else None
        except Exception as e:
            self.mgr.log.error(f'Version Tracker: {e}')
            config_dump = None

        # Determines type of upgrade by seeing what options were set
        upgrade_type = UpgradeType.FULL
        for key in params.keys():
            if key != 'image' and key != 'version' and params[key] is not None:
                upgrade_type = UpgradeType.STAGGERED
                break

        # Information is stored as standardized json entries
        new_entry = CephVersionEntry(
            version=version,
            upgrade_type=upgrade_type,
            command_options=params,
            status=status,
            config_dump=config_dump
        ).to_json()
        self.mgr.set_store(f'{VERSION_HISTORY_KEY_PREFIX}{time}', json.dumps(new_entry))
        self.mgr.log.info(f'Version Tracker: {VERSION_HISTORY_KEY_PREFIX}{time} entry added with version {version}')

    def update_cluster_version_status(self, status: UpgradeStatus) -> None:
        # Changes status field depending on if upgrade was completed or was stopped
        raw = self.mgr.get_store_prefix(VERSION_HISTORY_KEY_PREFIX)
        latest_entry_key, latest_entry_value = next(reversed(raw.items()))
        latest_entry_value = json.loads(latest_entry_value)
        latest_entry_value['status'] = status
        self.mgr.set_store(latest_entry_key, json.dumps(latest_entry_value))
        self.mgr.log.info(f'Version Tracker: {latest_entry_key} entry updated with status {status}')

    def get_cluster_version_history(self, show_config: Optional[bool] = False) -> str:
        raw = self.mgr.get_store_prefix(VERSION_HISTORY_KEY_PREFIX)
        prefix_len = len(VERSION_HISTORY_KEY_PREFIX)
        res = {k[prefix_len:]: json.loads(v) for (k, v) in raw.items()}
        if not res:
            return 'No Cluster Version History Stored'
        if show_config:
            return json.dumps(res, indent=4)
        else:
            for entry in res.values():
                del entry['config_dump']
            return json.dumps(res, indent=4)

    def remove_cluster_version_history(self, all: Optional[bool] = False, before: Optional[str] = None, after: Optional[str] = None) -> Tuple[int, str]:
        # Validate option combinations and datetime formats
        if not all and not before and not after:
            return -errno.EINVAL, 'requires at least one of the options "all", "before", "after" to be set'
        if all and (before or after):
            return -errno.EINVAL, 'cannot have option "all" set while option "before" or option "after" are set'
        if before:
            try:
                datetime.datetime.strptime(before, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return -errno.EINVAL, 'invalid datetime format for option "before", use "YYYY-MM-DD HH:MM:SS"'
        if after:
            try:
                datetime.datetime.strptime(after, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                return -errno.EINVAL, 'invalid datetime format for option "after", use "YYYY-MM-DD HH:MM:SS"'
        if before and after and before < after:
            return -errno.EINVAL, 'option "before" cannot be a datetime less than option "after", command will remove entries in range (AFTER, BEFORE)'

        # Fetch existing entries and delete based on the requested range
        raw = self.mgr.get_store_prefix(VERSION_HISTORY_KEY_PREFIX)
        prefix_len = len(VERSION_HISTORY_KEY_PREFIX)
        entry_dates = [key[prefix_len:] for key in raw.keys()]
        if all:
            for entry in entry_dates:
                self.mgr.set_store(f'{VERSION_HISTORY_KEY_PREFIX}{entry}', None)
        else:
            for entry in entry_dates:
                if before and entry >= before:
                    continue
                if after and entry <= after:
                    continue
                self.mgr.set_store(f'{VERSION_HISTORY_KEY_PREFIX}{entry}', None)
        return 0, 'Cluster Version History Deletion Successful'
