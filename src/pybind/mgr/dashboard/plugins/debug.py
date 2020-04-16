# -*- coding: utf-8 -*-
from __future__ import absolute_import

from enum import Enum
import json

from . import PLUGIN_MANAGER as PM
from . import interfaces as I  # noqa: E741,N812
from .plugin import SimplePlugin as SP

try:
    from typing import no_type_check
except ImportError:
    no_type_check = object()  # Just for type checking


class Actions(Enum):
    ENABLE = 'enable'
    DISABLE = 'disable'
    STATUS = 'status'


@PM.add_plugin  # pylint: disable=too-many-ancestors
class Debug(SP, I.CanCherrypy, I.ConfiguresCherryPy):  # pylint: disable=too-many-ancestors
    NAME = 'debug'

    OPTIONS = [
        SP.Option(
            name=NAME,
            default=False,
            type='bool',
            desc="Enable/disable debug options"
        )
    ]

    @no_type_check
    def handler(self, action):
        ret = 0
        msg = ''
        if action in [Actions.ENABLE.value, Actions.DISABLE.value]:
            self.set_option(self.NAME, action == Actions.ENABLE.value)
            self.mgr.update_cherrypy_config({})
        else:
            debug = self.get_option(self.NAME)
            msg = "Debug: '{}'".format('enabled' if debug else 'disabled')
        return (ret, msg, None)

    COMMANDS = [
        SP.Command(
            prefix="dashboard {name}".format(name=NAME),
            args="name=action,type=CephChoices,strings={states}".format(
                states="|".join(a.value for a in Actions)),
            desc="Control and report debug status in Ceph-Dashboard",
            handler=handler
        )
    ]

    def custom_error_response(self, status, message, traceback, version):
        self.response.headers['Content-Type'] = 'application/json'
        error_response = dict(status=status, detail=message, request_id=str(self.request.unique_id))

        if self.get_option(self.NAME):
            error_response.update(dict(traceback=traceback, version=version))

        return json.dumps(error_response)

    @PM.add_hook
    def configure_cherrypy(self, config):
        config.update({
            'environment': 'test_suite' if self.get_option(self.NAME) else 'production',
            'error_page.default': self.custom_error_response,
        })
