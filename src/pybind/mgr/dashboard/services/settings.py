# -*- coding: utf-8 -*-
from contextlib import contextmanager

import cherrypy


class SettingsService:
    @contextmanager
    # pylint: disable=no-self-argument
    def attribute_handler(name):
        """
        :type name: str|dict[str, str]
        :rtype: str|dict[str, str]
        """
        if isinstance(name, dict):
            result = {
                _to_native(key): value
                for key, value in name.items()
            }
        else:
            result = _to_native(name)

        try:
            yield result
        except AttributeError:  # pragma: no cover - handling is too obvious
            raise cherrypy.NotFound(result)  # pragma: no cover - handling is too obvious


def _to_native(setting):
    return setting.upper().replace('-', '_')
