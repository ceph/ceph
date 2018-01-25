# -*- coding: utf-8 -*-
from __future__ import absolute_import

import importlib
import os
import pkgutil
import sys


def ApiController(path):
    def decorate(cls):
        cls._cp_controller_ = True
        cls._cp_path_ = path
        if not hasattr(cls, '_cp_config'):
            cls._cp_config = {
                'tools.sessions.on': True,
                'tools.autenticate.on': False
            }
        else:
            cls._cp_config['tools.sessions.on'] = True
            if 'tools.autenticate.on' not in cls._cp_config:
                cls._cp_config['tools.autenticate.on'] = False
        return cls
    return decorate


def AuthRequired(enabled=True):
    def decorate(cls):
        if not hasattr(cls, '_cp_config'):
            cls._cp_config = {
                'tools.autenticate.on': enabled
            }
        else:
            cls._cp_config['tools.autenticate.on'] = enabled
        return cls
    return decorate


def load_controllers(mgrmodule):
    # setting sys.path properly when not running under the mgr
    dashboard_dir = os.path.dirname(os.path.realpath(__file__))
    mgr_dir = os.path.dirname(dashboard_dir)
    if mgr_dir not in sys.path:
        sys.path.append(mgr_dir)

    controllers = []
    ctrls_path = "{}/controllers".format(dashboard_dir)
    mods = [mod for _, mod, _ in pkgutil.iter_modules([ctrls_path])]
    for mod_name in mods:
        mod = importlib.import_module('.controllers.{}'.format(mod_name),
                                      package='dashboard_v2')
        for _, cls in mod.__dict__.items():
            if isinstance(cls, type) and hasattr(cls, '_cp_controller_'):
                # found controller
                cls._mgr_module_ = mgrmodule
                controllers.append(cls)

    return controllers


def load_controller(mgrmodule, cls):
    ctrls = load_controllers(mgrmodule)
    for ctrl in ctrls:
        if ctrl.__name__ == cls:
            return ctrl
    raise Exception("Controller class '{}' not found".format(cls))
