# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, RESTController
from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope


@ApiController('/telemetry', Scope.CONFIG_OPT)
class Telemetry(RESTController):

    @RESTController.Collection('GET')
    def report(self):
        """
        Get Ceph and device report data
        :return: Ceph and device report data
        :rtype: dict
        """
        return mgr.remote('telemetry', 'get_report', 'all')

    def singleton_set(self, enable=True, license_name=None):
        """
        Enables or disables sending data collected by the Telemetry
        module.
        :param enable: Enable or disable sending data
        :type enable: bool
        :param license_name: License string e.g. 'sharing-1-0' to
            make sure the user is aware of and accepts the license
            for sharing Telemetry data.
        :type license_name: string
        """
        if enable:
            if not license_name or (license_name != 'sharing-1-0'):
                raise DashboardException(
                    code='telemetry_enable_license_missing',
                    msg='Telemetry data is licensed under the Community Data License Agreement - '
                        'Sharing - Version 1.0 (https://cdla.io/sharing-1-0/). To enable, add '
                        '{"license": "sharing-1-0"} to the request payload.'
                )
            mgr.remote('telemetry', 'on')
        else:
            mgr.remote('telemetry', 'off')
