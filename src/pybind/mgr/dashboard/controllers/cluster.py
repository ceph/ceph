# -*- coding: utf-8 -*-

from ..security import Scope
from ..services.cluster import ClusterModel
from . import ApiController, ControllerDoc, EndpointDoc, RESTController


@ApiController('/cluster', Scope.CONFIG_OPT)
@ControllerDoc("Get Cluster Details", "Cluster")
class Cluster(RESTController):
    @RESTController.MethodMap(version='0.1')
    @EndpointDoc("Get the cluster status")
    def list(self):
        return ClusterModel.from_db().dict()

    @RESTController.MethodMap(version='0.1')
    @EndpointDoc("Update the cluster status",
                 parameters={'status': (str, 'Cluster Status')})
    def singleton_set(self, status: str):
        ClusterModel(status).to_db()
