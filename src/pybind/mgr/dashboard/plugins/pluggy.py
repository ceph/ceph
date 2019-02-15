# -*- coding: utf-8 -*-
"""
The MIT License (MIT)

Copyright (c) 2015 holger krekel (rather uses bitbucket/hpk42)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

"""
CAVEAT:
This is a minimal implementation of python-pluggy (based on 0.8.0 interface:
https://github.com/pytest-dev/pluggy/releases/tag/0.8.0).


Despite being a widely available Python library, it does not reach all the
distros and releases currently targeted for Ceph Nautilus:
- CentOS/RHEL 7.5 [ ]
- CentOS/RHEL 8 [ ]
- Debian 8.0 [ ]
- Debian 9.0 [ ]
- Ubuntu 14.05 [ ]
- Ubuntu 16.04 [X]

TODO: Once this becomes available in the above distros, this file should be
REMOVED, and the fully featured python-pluggy should be used instead.
"""


class HookspecMarker(object):
    """ Dummy implementation. No spec validation. """
    def __init__(self, project_name):
        self.project_name = project_name

    def __call__(self, function, *args, **kwargs):
        """ No options supported. """
        if any(args) or any(kwargs):
            raise NotImplementedError(
                "This is a minimal implementation of pluggy")
        return function


class HookimplMarker(object):
    def __init__(self, project_name):
        self.project_name = project_name

    def __call__(self, function, *args, **kwargs):
        """ No options supported."""
        if any(args) or any(kwargs):
            raise NotImplementedError(
                "This is a minimal implementation of pluggy")
        setattr(function, self.project_name + "_impl", {})
        return function


class _HookRelay(object):
    """
    Provides the PluginManager.hook.<method_name>() syntax and
    functionality.
    """
    def __init__(self):
        from collections import defaultdict
        self._registry = defaultdict(list)

    def __getattr__(self, hook_name):
        return lambda *args, **kwargs: [
            hook(*args, **kwargs) for hook in self._registry[hook_name]]

    def _add_hookimpl(self, hook_name, hook_method):
        self._registry[hook_name].append(hook_method)


class PluginManager(object):
    def __init__(self, project_name):
        self.project_name = project_name
        self.__hook = _HookRelay()

    @property
    def hook(self):
        return self.__hook

    def parse_hookimpl_opts(self, plugin, name):
        return getattr(
            getattr(plugin, name),
            self.project_name + "_impl",
            None)

    def add_hookspecs(self, module_or_class):
        """ Dummy method"""
        pass

    def register(self, plugin, name=None):
        for attr in dir(plugin):
            if self.parse_hookimpl_opts(plugin, attr) is not None:
                self.hook._add_hookimpl(attr, getattr(plugin, attr))
