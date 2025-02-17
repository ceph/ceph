# -*- coding: utf-8 -*-

from . import PLUGIN_MANAGER as PM  # pylint: disable=cyclic-import
from . import Interface, Mixin


class CanMgr(Mixin):
    from .. import mgr
    mgr = mgr  # type: ignore


class CanCherrypy(Mixin):
    import cherrypy
    request = cherrypy.request
    response = cherrypy.response


@PM.add_interface
class Initializable(Interface):
    @PM.add_abcspec
    def init(self):
        """
        Placeholder for module scope initialization
        """


@PM.add_interface
class Setupable(Interface):
    @PM.add_abcspec
    def setup(self):
        """
        Placeholder for plugin setup, right after server start.
        CanMgr.mgr is initialized by then.
        """


@PM.add_interface
class HasOptions(Interface):
    @PM.add_abcspec
    def get_options(self):
        pass


@PM.add_interface
class HasCommands(Interface):
    @PM.add_abcspec
    def register_commands(self):
        pass


@PM.add_interface
class HasControllers(Interface):
    @PM.add_abcspec
    def get_controllers(self):
        pass


@PM.add_interface
class ConfiguresCherryPy(Interface):
    @PM.add_abcspec
    def configure_cherrypy(self, config):
        pass


class FilterRequest(object):
    @PM.add_interface
    class BeforeHandler(Interface):
        @PM.add_abcspec
        def filter_request_before_handler(self, request):
            pass


@PM.add_interface
class ConfigNotify(Interface):
    @PM.add_abcspec
    def config_notify(self):
        """
        This method is called whenever a option of this mgr module has
        been modified.
        """
