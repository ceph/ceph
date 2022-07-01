# -*- coding: utf-8 -*-

from typing import Optional

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
