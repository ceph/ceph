# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import PLUGIN_MANAGER as PM, Interface


class CanMgr(Interface):
    from .. import mgr
    mgr = mgr


class CanLog(Interface):
    from .. import logger
    log = logger


@PM.add_interface
class Setupable(Interface):
    @PM.add_abcspec
    def setup(self):
        """
        Placeholder for plugin setup, right after server start.
        CanMgr.mgr and CanLog.log are initialized by then.
        """
        pass


@PM.add_interface
class HasOptions(Interface):
    @PM.add_abcspec
    def get_options(self): pass


@PM.add_interface
class HasCommands(Interface):
    @PM.add_abcspec
    def register_commands(self): pass


@PM.add_interface
class HasControllers(Interface):
    @PM.add_abcspec
    def get_controllers(self): pass


class FilterRequest:
    @PM.add_interface
    class BeforeHandler(Interface):
        @PM.add_abcspec
        def filter_request_before_handler(self, request): pass
