import logging
import threading
import os
import subprocess

from mgr_module import MgrModule, CLICommand, HandleCommandResult, Option
import orchestrator

from ceph.deployment.service_spec import RGWSpec

from typing import cast, Any, Optional, Sequence

from . import *
from ceph.rgw.types import RGWAMException, RGWAMEnvMgr
from ceph.rgw.rgwam_core import EnvArgs, RGWAM


class RGWAMOrchMgr(RGWAMEnvMgr):
    def __init__(self, mgr):
        self.mgr = mgr

    def tool_exec(self, prog, args):
        cmd = [ prog ] + args
        rc, stdout, stderr = self.mgr.tool_exec(args = cmd)
        return cmd, rc, stdout, stderr

    def apply_rgw(self, svc_id, realm_name, zone_name, port = None):
        spec = RGWSpec(service_id = svc_id,
                       rgw_realm = realm_name,
                       rgw_zone = zone_name,
                       rgw_frontend_port = port)
        completion = self.mgr.apply_rgw(spec)
        orchestrator.raise_if_exception(completion)

    def list_daemons(self, service_name, daemon_type = None, daemon_id = None, host = None, refresh = True):
        completion = self.mgr.list_daemons(service_name,
                                           daemon_type,
                                           daemon_id=daemon_id,
                                           host=host,
                                           refresh=refresh)
        return orchestrator.raise_if_exception(completion)


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
            self.env = EnvArgs(RGWAMOrchMgr(self))

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
        cmd, returncode, out, err = self.env.mgr.tool_exec('radosgw-admin', params or [])

        self.log.error('retcode=%d' % returncode)
        self.log.error('out=%s' % out)
        self.log.error('err=%s' % err)

        return HandleCommandResult(retval=returncode, stdout=out, stderr=err)

    @CLICommand('rgw realm bootstrap', perm='rw')
    def _cmd_rgw_realm_bootstrap(self,
                                 realm_name : Optional[str] = None,
                                 zonegroup_name: Optional[str] = None,
                                 zone_name: Optional[str] = None,
                                 endpoints: Optional[str] = None,
                                 sys_uid: Optional[str] = None,
                                 uid: Optional[str] = None,
                                 start_radosgw: Optional[bool] = True):
        """Bootstrap new rgw realm, zonegroup, and zone"""


        try:
            retval, out, err = RGWAM(self.env).realm_bootstrap(realm_name, zonegroup_name,
                    zone_name, endpoints, sys_uid, uid, start_radosgw)
        except RGWAMException as e:
            self.log.error('cmd run exception: (%d) %s' % (e.retcode, e.message))
            return (e.retcode, e.message, e.stderr)

        return HandleCommandResult(retval=retval, stdout=out, stderr=err)

    @CLICommand('rgw realm zone-creds create', perm='rw')
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

    @CLICommand('rgw realm zone-creds remove', perm='rw')
    def _cmd_rgw_realm_rm_zone_creds(self,
                                 realm_token : Optional[str] = None):
        """Create credentials for new zone creation"""

        try:
            retval, out, err = RGWAM(self.env).realm_rm_zone_creds(realm_token)
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
                             start_radosgw: Optional[bool] = True):
        """Bootstrap new rgw zone that syncs with existing zone"""

        try:
            retval, out, err = RGWAM(self.env).zone_create(realm_token, zonegroup_name,
                    zone_name, endpoints, start_radosgw)
        except RGWAMException as e:
            self.log.error('cmd run exception: (%d) %s' % (e.retcode, e.message))
            return (e.retcode, e.message, e.stderr)

        return HandleCommandResult(retval=retval, stdout=out, stderr=err)

    @CLICommand('rgw realm reconcile', perm='rw')
    def _cmd_rgw_realm_reconcile(self,
                             realm_name : Optional[str] = None,
                             zonegroup_name: Optional[str] = None,
                             zone_name: Optional[str] = None,
                             update: Optional[bool] = False):
        """Bootstrap new rgw zone that syncs with existing zone"""

        try:
            retval, out, err = RGWAM(self.env).realm_reconcile(realm_name, zonegroup_name,
                                                               zone_name, update)
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
