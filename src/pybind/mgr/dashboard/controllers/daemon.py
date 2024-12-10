# -*- coding: utf-8 -*-

from typing import List, Optional

from ..exceptions import DashboardException
from ..security import Scope
from ..services.exception import handle_orchestrator_error
from ..services.orchestrator import OrchClient, OrchFeature
from . import APIDoc, APIRouter, RESTController
from ._version import APIVersion
from .orchestrator import raise_if_no_orchestrator


@APIRouter('/daemon', Scope.HOSTS)
@APIDoc("Perform actions on daemons", "Daemon")
class Daemon(RESTController):
    @raise_if_no_orchestrator([OrchFeature.DAEMON_ACTION])
    @handle_orchestrator_error('daemon')
    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    def set(self, daemon_name: str, action: str = '',
            container_image: Optional[str] = None):

        if action not in ['start', 'stop', 'restart', 'redeploy']:
            raise DashboardException(
                code='invalid_daemon_action',
                msg=f'Daemon action "{action}" is either not valid or not supported.')
        # non 'None' container_images change need a redeploy
        if container_image == '' and action != 'redeploy':
            container_image = None

        orch = OrchClient.instance()
        res = orch.daemons.action(action=action, daemon_name=daemon_name, image=container_image)
        return res

    @raise_if_no_orchestrator([OrchFeature.DAEMON_LIST])
    @handle_orchestrator_error('daemon')
    @RESTController.MethodMap(version=APIVersion.DEFAULT)
    def list(self, daemon_types: Optional[List[str]] = None):
        """List all daemons in the cluster. Also filter by the daemon types specified

        :param daemon_types: List of daemon types to filter by.
        :return: Returns list of daemons.
        :rtype: list
        """
        orch = OrchClient.instance()
        daemons = [d.to_dict() for d in orch.services.list_daemons()]
        if daemon_types:
            daemons = [d for d in daemons if d['daemon_type'] in daemon_types]
        return daemons
