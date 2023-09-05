"""
Copyright (C) 2019 SUSE

LGPL2.1.  See file COPYING.
"""
import errno
import json
import sqlite3
from typing import Any, Dict, Optional, Tuple
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
        if not self.has_fs(fs):
            return -errno.EINVAL, '', f"no such file system: {fs}"
        return 0, fs, 'Success'

    def has_fs(self, fs_name: str) -> bool:
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
                          format: Optional[str] = 'plain') -> Tuple[int, str, str]:
        '''
        List current snapshot schedules
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        try:
            ret_scheds = self.client.get_snap_schedules(fs, path)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        if format == 'json':
            json_report = ','.join([ret_sched.report_json() for ret_sched in ret_scheds])
            return 0, f'[{json_report}]', ''
        return 0, '\n===\n'.join([ret_sched.report() for ret_sched in ret_scheds]), ''

    @CLIReadCommand('fs snap-schedule list')
    def snap_schedule_list(self, path: str,
                           recursive: bool = False,
                           fs: Optional[str] = None,
                           format: Optional[str] = 'plain') -> Tuple[int, str, str]:
        '''
        Get current snapshot schedule for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        try:
            scheds = self.client.list_snap_schedules(fs, path, recursive)
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
            out = {'path': path, 'schedule': schedule_list, 'retention': retention_list}
            return 0, json.dumps(out), ''
        return 0, '\n'.join([str(sched) for sched in scheds]), ''

    @CLIWriteCommand('fs snap-schedule add')
    def snap_schedule_add(self,
                          path: str,
                          snap_schedule: str,
                          start: Optional[str] = None,
                          fs: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Set a snapshot schedule for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        try:
            abs_path = path
            subvol = None
            self.client.store_snap_schedule(fs,
                                            abs_path,
                                            (abs_path, snap_schedule,
                                             fs, path, start, subvol))
            suc_msg = f'Schedule set for path {path}'
        except sqlite3.IntegrityError:
            existing_scheds = self.client.get_snap_schedules(fs, path)
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
                         fs: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Remove a snapshot schedule for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        try:
            abs_path = path
            self.client.rm_snap_schedule(fs, abs_path, repeat, start)
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        return 0, 'Schedule removed for path {}'.format(path), ''

    @CLIWriteCommand('fs snap-schedule retention add')
    def snap_schedule_retention_add(self,
                                    path: str,
                                    retention_spec_or_period: str,
                                    retention_count: Optional[str] = None,
                                    fs: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Set a retention specification for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        try:
            abs_path = path
            self.client.add_retention_spec(fs, abs_path,
                                           retention_spec_or_period,
                                           retention_count)
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        return 0, 'Retention added to path {}'.format(path), ''

    @CLIWriteCommand('fs snap-schedule retention remove')
    def snap_schedule_retention_rm(self,
                                   path: str,
                                   retention_spec_or_period: str,
                                   retention_count: Optional[str] = None,
                                   fs: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Remove a retention specification for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        try:
            abs_path = path
            self.client.rm_retention_spec(fs, abs_path,
                                          retention_spec_or_period,
                                          retention_count)
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        return 0, 'Retention removed from path {}'.format(path), ''

    @CLIWriteCommand('fs snap-schedule activate')
    def snap_schedule_activate(self,
                               path: str,
                               repeat: Optional[str] = None,
                               start: Optional[str] = None,
                               fs: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Activate a snapshot schedule for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        try:
            abs_path = path
            self.client.activate_snap_schedule(fs, abs_path, repeat, start)
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        return 0, 'Schedule activated for path {}'.format(path), ''

    @CLIWriteCommand('fs snap-schedule deactivate')
    def snap_schedule_deactivate(self,
                                 path: str,
                                 repeat: Optional[str] = None,
                                 start: Optional[str] = None,
                                 fs: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Deactivate a snapshot schedule for <path>
        '''
        rc, fs, err = self._validate_fs(fs)
        if rc < 0:
            return rc, fs, err
        try:
            abs_path = path
            self.client.deactivate_snap_schedule(fs, abs_path, repeat, start)
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except Exception as e:
            return -errno.EIO, '', str(e)
        return 0, 'Schedule deactivated for path {}'.format(path), ''
