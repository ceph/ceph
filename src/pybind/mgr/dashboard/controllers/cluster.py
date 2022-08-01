# -*- coding: utf-8 -*-

from ..security import Scope
from ..services.cluster import ClusterModel
from . import APIDoc, APIRouter, EndpointDoc, RESTController
from ._version import APIVersion


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
        ClusterModel(status).to_db()

    @RESTController.Collection('GET', 'capacity')
    def get_capacity(self):
        return ClusterModel.get_capacity()
