import logging
import threading
import os
import subprocess

from mgr_module import MgrModule, CLICommand, HandleCommandResult, Option

from typing import cast, Any, Optional, Sequence

log = logging.getLogger(__name__)


class Module(MgrModule):
    MODULE_OPTIONS = []

    # These are "native" Ceph options that this module cares about.
    NATIVE_OPTIONS = []

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)

        # set up some members to enable the serve() method and shutdown()
        self.run = True
        self.event = threading.Event()

        # ensure config options members are initialized; see config_notify()
        self.config_notify()

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
        log.error('self.config=%s' % str(self.get_ceph_conf_path()))
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

    def shutdown(self) -> None:
        """
        This method is called by the mgr when the module needs to shut
        down (i.e., when the serve() function needs to exit).
        """
        self.log.info('Stopping')
        self.run = False
        self.event.set()
