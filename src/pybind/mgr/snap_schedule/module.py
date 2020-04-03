"""
Copyright (C) 2019 SUSE

LGPL2.1.  See file COPYING.
"""
import errno
import json
import sqlite3
from .fs.schedule import SnapSchedClient, Schedule
from mgr_module import MgrModule, CLIReadCommand, CLIWriteCommand
from mgr_util import CephfsConnectionException
from rados import ObjectNotFound
from threading import Event


class Module(MgrModule):

    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self._initialized = Event()
        self.client = SnapSchedClient(self)

    def resolve_subvolume_path(self, fs, subvol, path):
        if not subvol:
            return path

        rc, subvol_path, err = self.remote('fs', 'subvolume', 'getpath',
                                           fs, subvol)
        if rc != 0:
            # TODO custom exception?
            raise Exception(f'Could not resolve {path} in {fs}, {subvol}')
        return subvol_path + path

    @property
    def default_fs(self):
        fs_map = self.get('fs_map')
        if fs_map['filesystems']:
            return fs_map['filesystems'][0]['mdsmap']['fs_name']
        else:
            self.log.error('No filesystem instance could be found.')
            raise CephfsConnectionException(
                -errno.ENOENT, "no filesystem found")

    def serve(self):
        self._initialized.set()

    def handle_command(self, inbuf, cmd):
        self._initialized.wait()
        return -errno.EINVAL, "", "Unknown command"

    @CLIReadCommand('fs snap-schedule status',
                    'name=path,type=CephString,req=false '
                    'name=subvol,type=CephString,req=false '
                    'name=fs,type=CephString,req=false',
                    'List current snapshot schedules')
    def snap_schedule_get(self, path='/', subvol=None, fs=None):
        use_fs = fs if fs else self.default_fs
        try:
            ret_scheds = self.client.get_snap_schedules(use_fs, path)
        except CephfsConnectionException as e:
            return e.to_tuple()
        return 0, '\n===\n'.join([ret_sched.report() for ret_sched in ret_scheds]), ''

    @CLIReadCommand('fs snap-schedule list',
                    'name=path,type=CephString '
                    'name=recursive,type=CephString,req=false '
                    'name=subvol,type=CephString,req=false '
                    'name=fs,type=CephString,req=false',
                    'Get current snapshot schedule for <path>')
    def snap_schedule_list(self, path, subvol=None, recursive=False, fs=None):
        try:
            use_fs = fs if fs else self.default_fs
            scheds = self.client.list_snap_schedules(use_fs, path, recursive)
            self.log.debug(f'recursive is {recursive}')
        except CephfsConnectionException as e:
            return e.to_tuple()
        if not scheds:
            return errno.ENOENT, '', f'SnapSchedule for {path} not found'
        return 0, json.dumps([[sched[1], sched[2]] for sched in scheds]), ''

    @CLIWriteCommand('fs snap-schedule add',
                     'name=path,type=CephString '
                     'name=snap-schedule,type=CephString '
                     'name=retention-policy,type=CephString,req=false '
                     'name=start,type=CephString,req=false '
                     'name=fs,type=CephString,req=false '
                     'name=subvol,type=CephString,req=false',
                     'Set a snapshot schedule for <path>')
    def snap_schedule_add(self,
                          path,
                          snap_schedule,
                          retention_policy='',
                          start=None,
                          fs=None,
                          subvol=None):
        try:
            use_fs = fs if fs else self.default_fs
            abs_path = self.resolve_subvolume_path(fs, subvol, path)
            sched = Schedule(abs_path, snap_schedule, retention_policy,
                             start, use_fs, subvol, path)
            self.client.store_snap_schedule(use_fs, sched)
            suc_msg = f'Schedule set for path {path}'
        except sqlite3.IntegrityError:
            existing_scheds = self.client.get_snap_schedules(use_fs, path)
            report = [s.report() for s in existing_scheds]
            error_msg = f'Found existing schedule {report}'
            self.log.error(error_msg)
            return errno.EEXISTS, '', error_msg
        except CephfsConnectionException as e:
            return e.to_tuple()
        return 0, suc_msg, ''

    @CLIWriteCommand('fs snap-schedule remove',
                     'name=path,type=CephString '
                     'name=repeat,type=CephString,req=false '
                     'name=start,type=CephString,req=false '
                     'name=subvol,type=CephString,req=false '
                     'name=fs,type=CephString,req=false',
                     'Remove a snapshot schedule for <path>')
    def snap_schedule_rm(self,
                         path,
                         repeat=None,
                         start=None,
                         subvol=None,
                         fs=None):
        try:
            use_fs = fs if fs else self.default_fs
            abs_path = self.resolve_subvolume_path(fs, subvol, path)
            self.client.rm_snap_schedule(use_fs, abs_path, repeat, start)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except ValueError as e:
            return errno.ENOENT, '', str(e)
        return 0, 'Schedule removed for path {}'.format(path), ''
