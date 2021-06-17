import logging
import threading
import os
import subprocess

from mgr_module import MgrModule, CLICommand, HandleCommandResult, Option
import orchestrator

from typing import cast, Any, Optional, Sequence

from . import *
from .types import RGWAMException
from .rgwam import EnvArgs, RGWAM


class Module(orchestrator.OrchestratorClientMixin, MgrModule):
    MODULE_OPTIONS = []

    # These are "native" Ceph options that this module cares about.
    NATIVE_OPTIONS = []

    def __init__(self, *args: Any, **kwargs: Any):
        self.inited = False
        self.lock = threading.Lock()
        super(Module, self).__init__(*args, **kwargs)

        # ensure config options members are initialized; see config_notify()
        self.config_notify()

        with self.lock:
            self.inited = True
            self.env = EnvArgs(self,
                               str(self.get_ceph_conf_path()),
                               f'mgr.{self.get_mgr_id()}',
                               str(self.get_ceph_option('keyring')))

        # set up some members to enable the serve() method and shutdown()
        self.run = True
        self.event = threading.Event()



    def config_notify(self) -> None:
        """
        This method is called whenever one of our config options is changed.
        """
        # This is some boilerplate that stores MODULE_OPTIONS in a class
        # member, so that, for instance, the 'emphatic' option is always
        # available as 'self.emphatic'.
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))
        # Do the same for the native options.
        for opt in self.NATIVE_OPTIONS:
            setattr(self,
                    opt,
                    self.get_ceph_option(opt))
            self.log.debug(' native option %s = %s', opt, getattr(self, opt))


    @CLICommand('rgw admin', perm='rw')
    def _cmd_rgw_admin(self, params: Sequence[str]):
        """rgw admin"""
        run_cmd = [ './bin/radosgw-admin',
                    '-c', str(self.get_ceph_conf_path()),
                    '-k', str(self.get_ceph_option('keyring')),
                    '-n', f'mgr.{self.get_mgr_id()}' ] + (params or [])

        result = subprocess.run(run_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # TODO Extend export creation for rgw.
        out = result.stdout.decode('utf-8')
        err = result.stderr.decode('utf-8')
        self.log.error('retcode=%d' % result.returncode)
        self.log.error('out=%s' % out)
        self.log.error('err=%s' % err)

        return HandleCommandResult(retval=result.returncode, stdout=out, stderr=err)

    @CLICommand('rgw realm bootstrap', perm='rw')
    def _cmd_rgw_realm_bootstrap(self,
                                 realm_name : Optional[str] = None,
                                 zonegroup_name: Optional[str] = None,
                                 zone_name: Optional[str] = None,
                                 endpoints: Optional[str] = None,
                                 sys_uid: Optional[str] = None,
                                 uid: Optional[str] = None,
                                 start_radosgw: Optional[bool] = False):
        """Bootstrap new rgw realm, zonegroup, and zone"""


        try:
            retval, out, err = RGWAM(self.env).realm_bootstrap(realm_name, zonegroup_name,
                    zone_name, endpoints, sys_uid, uid, start_radosgw)
        except RGWAMException as e:
            self.log.error('cmd run exception: (%d) %s' % (e.retcode, e.message))
            return (e.retcode, e.message, e.stderr)

        return HandleCommandResult(retval=retval, stdout=out, stderr=err)

    @CLICommand('rgw realm create zone-creds', perm='rw')
    def _cmd_rgw_realm_new_zone_creds(self,
                                 realm_name: Optional[str] = None,
                                 endpoints: Optional[str] = None,
                                 sys_uid: Optional[str] = None):
        """Create credentials for new zone creation"""

        try:
            retval, out, err = RGWAM(self.env).realm_new_zone_creds(realm_name, endpoints, sys_uid)
        except RGWAMException as e:
            self.log.error('cmd run exception: (%d) %s' % (e.retcode, e.message))
            return (e.retcode, e.message, e.stderr)

        return HandleCommandResult(retval=retval, stdout=out, stderr=err)

    @CLICommand('rgw zone create', perm='rw')
    def _cmd_rgw_zone_create(self,
                             realm_token : Optional[str] = None,
                             zonegroup_name: Optional[str] = None,
                             zone_name: Optional[str] = None,
                             endpoints: Optional[str] = None,
                             start_radosgw: Optional[bool] = False):
        """Bootstrap new rgw zone that syncs with existing zone"""

        try:
            retval, out, err = RGWAM(self.env).zone_create(realm_token, zonegroup_name,
                    zone_name, endpoints, start_radosgw)
        except RGWAMException as e:
            self.log.error('cmd run exception: (%d) %s' % (e.retcode, e.message))
            return (e.retcode, e.message, e.stderr)

        return HandleCommandResult(retval=retval, stdout=out, stderr=err)

    def shutdown(self) -> None:
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        self.log.info('Stopping')
        self.run = False
        self.event.set()
