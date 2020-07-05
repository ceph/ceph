# -*- coding: utf-8 -*-
from __future__ import absolute_import

import abc

from .pluggy import HookspecMarker, HookimplMarker, PluginManager


class Interface(object, metaclass=abc.ABCMeta):
    pass


class Mixin(object):
    pass


class DashboardPluginManager(object):
    def __init__(self, project_name):
        self.__pm = PluginManager(project_name)
        self.__add_spec = HookspecMarker(project_name)
        self.__add_abcspec = lambda *args, **kwargs: abc.abstractmethod(
            self.__add_spec(*args, **kwargs))
        self.__add_hook = HookimplMarker(project_name)

    pm = property(lambda self: self.__pm)
    hook = property(lambda self: self.pm.hook)

    add_spec = property(lambda self: self.__add_spec)
    add_abcspec = property(lambda self: self.__add_abcspec)
    add_hook = property(lambda self: self.__add_hook)

    def add_interface(self, cls):
        assert issubclass(cls, Interface)
        self.pm.add_hookspecs(cls)
        return cls

    @staticmethod
    def final(func):
        setattr(func, '__final__', True)
        return func

    def add_plugin(self, plugin):
        """ Provides decorator interface for PluginManager.register():
            @PLUGIN_MANAGER.add_plugin
            class Plugin(...):
                ...
        Additionally it checks whether the Plugin instance has all Interface
        methods implemented and marked with add_hook decorator.
        As a con of this approach, plugins cannot call super() from __init__()
        """
        assert issubclass(plugin, Interface)
        from inspect import getmembers, ismethod
        for interface in plugin.__bases__:
            for method_name, _ in getmembers(interface, predicate=ismethod):
                if hasattr(getattr(interface, method_name), '__final__'):
                    continue

                if self.pm.parse_hookimpl_opts(plugin, method_name) is None:
                    raise NotImplementedError(
                        "Plugin '{}' implements interface '{}' but existing"
                        " method '{}' is not declared added as hook".format(
                            plugin.__name__,
                            interface.__name__,
                            method_name))
        self.pm.register(plugin())
        return plugin


PLUGIN_MANAGER = DashboardPluginManager("ceph-mgr.dashboard")

# Load all interfaces and their hooks
from . import interfaces  # noqa: F401 pylint: disable=wrong-import-position,cyclic-import
