"""
Copyright (C) 2019 SUSE

LGPL2.1.  See file COPYING.
"""
import errno
import json
import sqlite3
from typing import Any, Dict, Optional, Tuple, Union
from .fs.schedule_client import SnapSchedClient
from mgr_module import MgrModule, CLIReadCommand, CLIWriteCommand, Option
from mgr_util import CephfsConnectionException
from threading import Event


class Module(MgrModule):
    MODULE_OPTIONS = [
        Option(
            'allow_m_granularity',
            type='bool',
            default=False,
            desc='allow minute scheduled snapshots',
            runtime=True,
        ),
        Option(
            'dump_on_update',
            type='bool',
            default=False,
            desc='dump database to debug log on update',
            runtime=True,
        ),

    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        self._initialized = Event()
        self.client = SnapSchedClient(self)

    def _subvolume_exist(self, fs: str, subvol: Union[str, None], group: Union[str, None]) -> bool:
        rc, subvolumes, err = self.remote('volumes', 'subvolume_ls', fs, group)
        if rc == 0:
            for svj in json.loads(subvolumes):
                if subvol == svj['name']:
                    return True

        return False

    def _subvolume_flavor(self, fs: str, subvol: Union[str, None], group: Union[str, None]) -> str:
        rc, subvol_info, err = self.remote('volumes', 'subvolume_info', fs, subvol, group)
        svi_json = json.loads(subvol_info)
        return svi_json.get('flavor', 'bad_flavor')  # 1 or 2 etc.

    def _resolve_subvolume_path(self, fs: str, path: str, subvol: Union[str, None], group: Union[str, None]) -> Tuple[int, str, str]:
        if subvol is None and group is None:
            return 0, path, ''

        rc = -1
        subvol_path = ''
        if self._subvolume_exist(fs, subvol, group):
            rc, subvol_path, err = self.remote('volumes', 'subvolume_getpath', fs, subvol, group)
            if rc != 0:
                return rc, '', f'Could not resolve subvol:{subvol} path in fs:{fs}'
            else:
                subvol_flavor = self._subvolume_flavor(fs, subvol, group)
                if subvol_flavor == 1:  # v1
                    return 0, subvol_path, f'Ignoring user specified path:{path} for subvol'
                if subvol_flavor == 2:  # v2
                    err = '';
                    if path != "/..":
                        err = f'Ignoring user specified path:{path} for subvol'
                    return 0, subvol_path + "/..", err

                return -errno.EINVAL, '', f'Unhandled subvol flavor:{subvol_flavor}'
        else:
            return -errno.EINVAL, '', f'No such subvol: {group}::{subvol}'

    @property
    def _default_fs(self) -> Tuple[int, str, str]:
        fs_map = self.get('fs_map')
        if len(fs_map['filesystems']) > 1:
            return -errno.EINVAL, '', "filesystem argument is required when there is more than one file system"
        elif len(fs_map['filesystems']) == 1:
            return 0, fs_map['filesystems'][0]['mdsmap']['fs_name'], "Success"
        else:
            self.log.error('No filesystem instance could be found.')
            return -errno.ENOENT, "", "no filesystem found"

    def _validate_fs(self, fs: Optional[str]) -> Tuple[int, str, str]:
        if not fs:
            rc, fs, err = self._default_fs
            if rc < 0:
                return rc, fs, err
        if not self._has_fs(fs):
            return -errno.EINVAL, '', f"no such file system: {fs}"
        return 0, fs, 'Success'

    def _has_fs(self, fs_name: str) -> bool:
        return fs_name in self.client.get_all_filesystems()

    def serve(self) -> None:
        self._initialized.set()

    def handle_command(self, inbuf: str, cmd: Dict[str, str]) -> Tuple[int, str, str]:
        self._initialized.wait()
        return -errno.EINVAL, "", "Unknown command"

    @CLIReadCommand('fs snap-schedule status')
    def snap_schedule_get(self,
                          path: str = '/',
                          fs: Optional[str] = None,
                          subvol: Optional[str] = None,
                          group: Optional[str] = None,
                          format: Optional[str] = 'plain') -> Tuple[int, str, str]:
        '''
        List current snapshot schedules
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        errstr = 'Success'
        try:
            rc, abs_path, errstr = self._resolve_subvolume_path(fs, path, subvol, group)
            if rc != 0:
                return rc, '', errstr
            ret_scheds = self.client.get_snap_schedules(fs, abs_path)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        if format == 'json':
            json_report = ','.join([ret_sched.report_json() for ret_sched in ret_scheds])
            return 0, f'[{json_report}]', ''
        self.log.info(errstr)
        return 0, '\n===\n'.join([ret_sched.report() for ret_sched in ret_scheds]), ''

    @CLIReadCommand('fs snap-schedule list')
    def snap_schedule_list(self, path: str,
                           recursive: bool = False,
                           fs: Optional[str] = None,
                           subvol: Optional[str] = None,
                           group: Optional[str] = None,
                           format: Optional[str] = 'plain') -> Tuple[int, str, str]:
        '''
        Get current snapshot schedule for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        abs_path = ""
        try:
            rc, abs_path, errstr = self._resolve_subvolume_path(fs, path, subvol, group)
            if rc != 0:
                return rc, '', errstr
            scheds = self.client.list_snap_schedules(fs, abs_path, recursive)
            self.log.debug(f'recursive is {recursive}')
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        if not scheds:
            if format == 'json':
                output: Dict[str, str] = {}
                return 0, json.dumps(output), ''
            return -errno.ENOENT, '', f'SnapSchedule for {path} not found'
        if format == 'json':
            # json_list = ','.join([sched.json_list() for sched in scheds])
            schedule_list = [sched.schedule for sched in scheds]
            retention_list = [sched.retention for sched in scheds]
            out = {'path': abs_path, 'schedule': schedule_list, 'retention': retention_list}
            return 0, json.dumps(out), ''
        return 0, '\n'.join([str(sched) for sched in scheds]), ''

    @CLIWriteCommand('fs snap-schedule add')
    def snap_schedule_add(self,
                          path: str,
                          snap_schedule: str,
                          start: Optional[str] = None,
                          fs: Optional[str] = None,
                          subvol: Optional[str] = None,
                          group: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Set a snapshot schedule for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        abs_path = ""
        try:
            rc, abs_path, errstr = self._resolve_subvolume_path(fs, path, subvol, group)
            if rc != 0:
                return rc, '', errstr
            self.client.store_snap_schedule(fs,
                                            abs_path,
                                            (abs_path, snap_schedule,
                                             fs, abs_path, start, subvol, group))
            suc_msg = f'Schedule set for path {abs_path}'
        except sqlite3.IntegrityError:
            existing_scheds = self.client.get_snap_schedules(fs, abs_path)
            report = [s.report() for s in existing_scheds]
            error_msg = f'Found existing schedule {report}'
            self.log.error(error_msg)
            return -errno.EEXIST, '', error_msg
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        return 0, suc_msg, ''

    @CLIWriteCommand('fs snap-schedule remove')
    def snap_schedule_rm(self,
                         path: str,
                         repeat: Optional[str] = None,
                         start: Optional[str] = None,
                         fs: Optional[str] = None,
                         subvol: Optional[str] = None,
                         group: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Remove a snapshot schedule for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        abs_path = ""
        try:
            rc, abs_path, errstr = self._resolve_subvolume_path(fs, path, subvol, group)
            if rc != 0:
                return rc, '', errstr
            self.client.rm_snap_schedule(fs, abs_path, repeat, start)
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        return 0, 'Schedule removed for path {}'.format(abs_path), ''

    @CLIWriteCommand('fs snap-schedule retention add')
    def snap_schedule_retention_add(self,
                                    path: str,
                                    retention_spec_or_period: str,
                                    retention_count: Optional[str] = None,
                                    fs: Optional[str] = None,
                                    subvol: Optional[str] = None,
                                    group: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Set a retention specification for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        abs_path = ""
        try:
            rc, abs_path, errstr = self._resolve_subvolume_path(fs, path, subvol, group)
            if rc != 0:
                return rc, '', errstr
            self.client.add_retention_spec(fs, abs_path,
                                           retention_spec_or_period,
                                           retention_count)
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        return 0, 'Retention added to path {}'.format(abs_path), ''

    @CLIWriteCommand('fs snap-schedule retention remove')
    def snap_schedule_retention_rm(self,
                                   path: str,
                                   retention_spec_or_period: str,
                                   retention_count: Optional[str] = None,
                                   fs: Optional[str] = None,
                                   subvol: Optional[str] = None,
                                   group: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Remove a retention specification for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        abs_path = ""
        try:
            rc, abs_path, errstr = self._resolve_subvolume_path(fs, path, subvol, group)
            if rc != 0:
                return rc, '', errstr
            self.client.rm_retention_spec(fs, abs_path,
                                          retention_spec_or_period,
                                          retention_count)
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        return 0, 'Retention removed from path {}'.format(abs_path), ''

    @CLIWriteCommand('fs snap-schedule activate')
    def snap_schedule_activate(self,
                               path: str,
                               repeat: Optional[str] = None,
                               start: Optional[str] = None,
                               fs: Optional[str] = None,
                               subvol: Optional[str] = None,
                               group: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Activate a snapshot schedule for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        abs_path = ""
        try:
            rc, abs_path, errstr = self._resolve_subvolume_path(fs, path, subvol, group)
            if rc != 0:
                return rc, '', errstr
            self.client.activate_snap_schedule(fs, abs_path, repeat, start)
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        return 0, 'Schedule activated for path {}'.format(abs_path), ''

    @CLIWriteCommand('fs snap-schedule deactivate')
    def snap_schedule_deactivate(self,
                                 path: str,
                                 repeat: Optional[str] = None,
                                 start: Optional[str] = None,
                                 fs: Optional[str] = None,
                                 subvol: Optional[str] = None,
                                 group: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Deactivate a snapshot schedule for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        abs_path = ""
        try:
            rc, abs_path, errstr = self._resolve_subvolume_path(fs, path, subvol, group)
            if rc != 0:
                return rc, '', errstr
            self.client.deactivate_snap_schedule(fs, abs_path, repeat, start)
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        return 0, 'Schedule deactivated for path {}'.format(abs_path), ''
