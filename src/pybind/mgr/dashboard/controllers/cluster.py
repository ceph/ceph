# -*- coding: utf-8 -*-

from typing import Dict, List, Optional

from ..security import Scope
from ..services.cluster import ClusterModel
from ..services.exception import handle_orchestrator_error
from ..services.orchestrator import OrchClient, OrchFeature
from ..tools import str_to_bool
from . import APIDoc, APIRouter, CreatePermission, Endpoint, EndpointDoc, \
    ReadPermission, RESTController, UpdatePermission, allow_empty_body
from ._version import APIVersion
from .orchestrator import raise_if_no_orchestrator


@APIRouter('/cluster', Scope.CONFIG_OPT)
@APIDoc("Get Cluster Details", "Cluster")
class Cluster(RESTController):
    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    @EndpointDoc("Get the cluster status")
    def list(self):
        return ClusterModel.from_db().dict()

    @RESTController.MethodMap(version=APIVersion.EXPERIMENTAL)
    @EndpointDoc("Update the cluster status",
                 parameters={'status': (str, 'Cluster Status')})
    def singleton_set(self, status: str):
        ClusterModel(status).to_db()  # -*- coding: utf-8 -*-


@APIRouter('/cluster/upgrade', Scope.CONFIG_OPT)
@APIDoc("Upgrade Management API", "Upgrade")
class ClusterUpgrade(RESTController):
    @RESTController.MethodMap()
    @raise_if_no_orchestrator([OrchFeature.UPGRADE_LIST])
    @handle_orchestrator_error('upgrade')
    @EndpointDoc("Get the available versions to upgrade",
                 parameters={
                     'image': (str, 'Ceph Image'),
                     'tags': (bool, 'Show all image tags'),
                     'show_all_versions': (bool, 'Show all available versions')
                 })
    @ReadPermission
    def list(self, tags: bool = False, image: Optional[str] = None,
             show_all_versions: Optional[bool] = False) -> Dict:
        orch = OrchClient.instance()
        available_upgrades = orch.upgrades.list(image, str_to_bool(tags),
                                                str_to_bool(show_all_versions))
        return available_upgrades

    @Endpoint()
    @raise_if_no_orchestrator([OrchFeature.UPGRADE_STATUS])
    @handle_orchestrator_error('upgrade')
    @EndpointDoc("Get the cluster upgrade status")
    @ReadPermission
    def status(self) -> Dict:
        orch = OrchClient.instance()
        status = orch.upgrades.status().to_json()
        return status

    @Endpoint('POST')
    @raise_if_no_orchestrator([OrchFeature.UPGRADE_START])
    @handle_orchestrator_error('upgrade')
    @EndpointDoc("Start the cluster upgrade")
    @CreatePermission
    def start(self, image: Optional[str] = None, version: Optional[str] = None,
              daemon_types: Optional[List[str]] = None, host_placement: Optional[str] = None,
              services: Optional[List[str]] = None, limit: Optional[int] = None) -> str:
        orch = OrchClient.instance()
        start = orch.upgrades.start(image, version, daemon_types, host_placement, services, limit)
        return start

    @Endpoint('PUT')
    @raise_if_no_orchestrator([OrchFeature.UPGRADE_PAUSE])
    @handle_orchestrator_error('upgrade')
    @EndpointDoc("Pause the cluster upgrade")
    @UpdatePermission
    @allow_empty_body
    def pause(self) -> str:
        orch = OrchClient.instance()
        return orch.upgrades.pause()

    @Endpoint('PUT')
    @raise_if_no_orchestrator([OrchFeature.UPGRADE_RESUME])
    @handle_orchestrator_error('upgrade')
    @EndpointDoc("Resume the cluster upgrade")
    @UpdatePermission
    @allow_empty_body
    def resume(self) -> str:
        orch = OrchClient.instance()
        return orch.upgrades.resume()

    @Endpoint('PUT')
    @raise_if_no_orchestrator([OrchFeature.UPGRADE_STOP])
    @handle_orchestrator_error('upgrade')
    @EndpointDoc("Stop the cluster upgrade")
    @UpdatePermission
    @allow_empty_body
    def stop(self) -> str:
        orch = OrchClient.instance()
        return orch.upgrades.stop()
