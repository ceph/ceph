# -*- coding: utf-8 -*-
from __future__ import absolute_import
from contextlib import contextmanager

import cherrypy

from . import ApiController, RESTController
from ..settings import Settings as SettingsModule, Options
from ..security import Scope


@ApiController('/settings', Scope.CONFIG_OPT)
class Settings(RESTController):
    """
    Enables to manage the settings of the dashboard (not the Ceph cluster).
    """
    @contextmanager
    def _attribute_handler(self, name):
        """
        :type name: str|dict[str, str]
        :rtype: str|dict[str, str]
        """
        if isinstance(name, dict):
            result = {self._to_native(key): value
                      for key, value in name.items()}
        else:
            result = self._to_native(name)

        try:
            yield result
        except AttributeError:
            raise cherrypy.NotFound(result)

    @staticmethod
    def _to_native(setting):
        return setting.upper().replace('-', '_')

    def list(self):
        return [
            self._get(name) for name in Options.__dict__
            if name.isupper() and not name.startswith('_')
        ]

    def _get(self, name):
        with self._attribute_handler(name) as sname:
            default, data_type = getattr(Options, sname)
        return {
            'name': sname,
            'default': default,
            'type': data_type.__name__,
            'value': getattr(SettingsModule, sname)
        }

    def get(self, name):
        return self._get(name)

    def set(self, name, value):
        with self._attribute_handler(name) as sname:
            setattr(SettingsModule, self._to_native(sname), value)

    def delete(self, name):
        with self._attribute_handler(name) as sname:
            delattr(SettingsModule, self._to_native(sname))

    def bulk_set(self, **kwargs):
        with self._attribute_handler(kwargs) as data:
            for name, value in data.items():
                setattr(SettingsModule, self._to_native(name), value)
